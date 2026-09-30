package com.changgeng.writer;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class WalFileWriter {

    @Value("${wal.file.output-dir:./data/wal-logs}")
    private String outputDir;

    // 5分钟窗口
    private static final long WINDOW_MS = 5 * 60 * 1000L;
    private static final DateTimeFormatter FILE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmm").withZone(ZoneId.systemDefault());

    // 刷盘间隔与批次大小
    private static final long FLUSH_INTERVAL_MS = 3000L;
    private static final int BATCH_SIZE = 500;

    private Path outputDirPath;

    // 每个消费线程/分区独立的缓冲区和Writer，避免全局锁
    private final ConcurrentHashMap<Integer, PartitionWriter> partitionWriters = new ConcurrentHashMap<>();

    // 异步刷盘线程池
    private ScheduledExecutorService flushExecutor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    @PostConstruct
    public void init() throws IOException {
        outputDirPath = Paths.get(outputDir);
        Files.createDirectories(outputDirPath);

        // 单线程定时刷盘，保证刷盘顺序且不与写入竞争
        flushExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "wal-flush-thread");
            t.setDaemon(true);
            return t;
        });

        flushExecutor.scheduleAtFixedRate(this::flushAllPartitions, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("WalFileWriter(高并发版) 初始化完成，输出目录: {}, 刷盘间隔: {}ms", outputDirPath.toAbsolutePath(), FLUSH_INTERVAL_MS);
    }

    /**
     * 高并发写入入口
     * @param message 消息内容
     * @param partition Kafka分区号（用于隔离写入）
     */
    public void write(String message, int partition) {
        PartitionWriter writer = partitionWriters.computeIfAbsent(partition, k -> new PartitionWriter(k));
        writer.append(message);
    }

    /**
     * 兼容无分区信息的调用（降级为单线程写入）
     */
    public void write(String message) {
        write(message, 0);
    }

    /**
     * 定时刷盘任务：遍历所有分区的缓冲区，批量写入磁盘
     */
    private void flushAllPartitions() {
        if (!running.get()) return;

        for (PartitionWriter pw : partitionWriters.values()) {
            try {
                pw.flushIfNeeded();
            } catch (Exception e) {
                log.error("分区 {} 刷盘失败", pw.partitionId, e);
            }
        }
    }

    @PreDestroy
    public void destroy() {
        running.set(false);
        log.info("WalFileWriter 正在关闭，执行最终刷盘...");

        // 停止定时刷盘
        if (flushExecutor != null) {
            flushExecutor.shutdown();
            try {
                if (!flushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    flushExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                flushExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 强制刷出所有剩余缓冲
        for (PartitionWriter pw : partitionWriters.values()) {
            try {
                pw.forceFlushAndClose();
            } catch (IOException e) {
                log.error("关闭分区 {} Writer 失败", pw.partitionId, e);
            }
        }
        partitionWriters.clear();
        log.info("WalFileWriter 已安全关闭");
    }

    // ==================== 内部类：分区级写入器 ====================
    private class PartitionWriter {
        private final List<String> buffer = new CopyOnWriteArrayList<>();
        private BufferedWriter currentWriter;
        private String currentWindowFileName;
        private final int partitionId;

        PartitionWriter(int partitionId) {
            this.partitionId = partitionId;
        }

        // 仅在本分区内加锁，不同分区完全并行
        private final Object lock = new Object();

        void append(String message) {
            buffer.add(message);
            // 缓冲区满时主动触发刷盘，避免内存溢出
            if (buffer.size() >= BATCH_SIZE) {
                try {
                    flushIfNeeded();
                } catch (IOException e) {
                    log.error("分区 {} 主动刷盘失败", partitionId, e);
                }
            }
        }

        void flushIfNeeded() throws IOException {
            synchronized (lock) {
                if (buffer.isEmpty()) return;

                // 批量取出并清空缓冲
                List<String> batch = List.copyOf(buffer);
                buffer.clear();

                // 检查时间窗口是否切换
                rotateWriterIfNeeded();

                // 批量写入
                for (String msg : batch) {
                    currentWriter.write(msg);
                    currentWriter.newLine();
                }
                currentWriter.flush(); // 批量刷盘
            }
        }

        void forceFlushAndClose() throws IOException {
            synchronized (lock) {
                flushIfNeeded();
                if (currentWriter != null) {
                    currentWriter.close();
                    currentWriter = null;
                    log.info("分区 {} WAL文件已关闭: {}", partitionId, currentWindowFileName);
                }
            }
        }

        private void rotateWriterIfNeeded() throws IOException {
            String targetFileName = getCurrentWindowFileName();
            if (currentWriter == null || !targetFileName.equals(currentWindowFileName)) {
                if (currentWriter != null) {
                    currentWriter.close();
                    log.info("分区 {} WAL文件切换: {} -> {}", partitionId, currentWindowFileName, targetFileName);
                }
                currentWindowFileName = targetFileName;
                Path filePath = outputDirPath.resolve(currentWindowFileName);
                currentWriter = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        }

        private String getCurrentWindowFileName() {
            long now = System.currentTimeMillis();
            long windowStart = (now / WINDOW_MS) * WINDOW_MS;
            String formattedTime = FILE_FMT.format(Instant.ofEpochMilli(windowStart));
            // 文件名加入分区号，避免多分区写入同一文件产生冲突
            return String.format("wal-%s-p%d.wal", formattedTime, partitionId);
        }
    }
}
