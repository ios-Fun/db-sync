package com.changgeng.writer;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * WAL 文件摆渡写入器（一区）。
 *
 * 摆渡协议：
 * 1. 每 5 分钟一个时间窗口（窗口对齐自然时间），每个 Kafka 分区每个窗口一个文件，
 *    命名 wal-yyyyMMdd-HHmm-pN.wal（例如 09:55 窗口文件 wal-20260930-0955-p0.wal）。
 * 2. 窗口结束时刻（如 10:00:00）立即 flush + close + 原子改名为正式 .wal 文件，
 *    此后不再写入；网闸在窗口结束后取走该文件摆渡至三区。
 * 3. 三区在窗口结束 + completion-safety-ms(30s) 后才按文件名判定文件已完成并读取入库，
 *    因此本侧必须保证：正式 .wal 文件一旦出现即为最终内容、句柄已释放。
 *
 * 实现要点：
 * - 消息按“到达本方法的时刻”归属窗口，9:59:59 到达的消息即使 10:00 后才刷盘，也写入 9:55 文件。
 * - 写入期间文件位于 .staging 子目录（同名），窗口定稿后才原子移动到 ready 子目录，
 *    网闸只从 ready 目录取文件，杜绝摆渡到半成品。
 * - 每个(分区,窗口)独立文件与锁，不同分区、不同窗口互不阻塞。
 */
@Slf4j
@Component
public class WalFileWriter {

    @Value("${wal.file.output-dir:./data/wal-logs}")
    private String outputDir;

    /** 时间窗口大小(毫秒)，键名必须与三区 wal.window-ms 完全一致，默认 5 分钟 */
    @Value("${wal.window-ms:300000}")
    private long windowMs;

    /** 定时刷盘 / 边界定稿检查间隔(毫秒)，决定窗口结束后最迟多久定稿 */
    @Value("${wal.file.flush-interval-ms:3000}")
    private long flushIntervalMs;

    /** 单文件内存缓冲条数，达到后主动刷盘 */
    private static final int BATCH_SIZE = 500;

    private static final DateTimeFormatter FILE_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmm").withZone(ZoneId.systemDefault());

    private static final String STAGING_DIR = ".staging";
    /** 定稿后的正式文件目录，网闸从此目录取文件摆渡 */
    private static final String READY_DIR = "ready";
    private static final String WAL_SUFFIX = ".wal";
    private static final String WAL_FILE_PREFIX = "wal-";

    private Path outputDirPath;
    private Path stagingDirPath;
    private Path readyDirPath;

    // 每个 Kafka 分区独立一组窗口文件，分区之间完全并行
    private final ConcurrentHashMap<Integer, PartitionWriter> partitionWriters = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(true);

    @PostConstruct
    public void init() throws IOException {
        outputDirPath = Paths.get(outputDir);
        stagingDirPath = outputDirPath.resolve(STAGING_DIR);
        readyDirPath = outputDirPath.resolve(READY_DIR);
        Files.createDirectories(stagingDirPath);
        Files.createDirectories(readyDirPath);

        // 恢复上次进程残留的 staging 文件：已过窗口的立即定稿，窗口未结束的保留原位续写
        recoverStagingFiles();

        // 单线程定时任务：刷当前窗口缓冲 + 定稿刚结束的窗口
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "wal-flush-thread");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::tick, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
        log.info("WalFileWriter 初始化完成: 输出目录={}, 窗口={}ms, 刷盘/定稿间隔={}ms",
                outputDirPath.toAbsolutePath(), windowMs, flushIntervalMs);
    }

    /**
     * 高并发写入入口
     * @param message   消息内容
     * @param partition Kafka 分区号（用于隔离写入）
     */
    public void write(String message, int partition) {
        long now = System.currentTimeMillis();
        long windowStart = (now / windowMs) * windowMs;
        partitionWriters.computeIfAbsent(partition, PartitionWriter::new).append(message, windowStart);
    }

    /** 兼容无分区信息的调用（降级为分区0） */
    public void write(String message) {
        write(message, 0);
    }

    /**
     * 定时任务：
     * 1. 各窗口缓冲批量刷入 .staging 临时文件（崩溃最多丢一个间隔的数据）；
     * 2. 将已结束窗口的临时文件 close 并原子改名为正式 .wal，供网闸摆渡。
     */
    private void tick() {
        if (!running.get()) {
            return;
        }
        long now = System.currentTimeMillis();
        for (PartitionWriter pw : partitionWriters.values()) {
            try {
                pw.tick(now);
            } catch (Exception e) {
                log.error("分区 {} 刷盘/定稿失败", pw.partitionId, e);
            }
        }
    }

    @PreDestroy
    public void destroy() {
        running.set(false);
        log.info("WalFileWriter 正在关闭，执行最终刷盘...");

        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        long now = System.currentTimeMillis();
        for (PartitionWriter pw : partitionWriters.values()) {
            try {
                pw.shutdown(now);
            } catch (Exception e) {
                log.error("关闭分区 {} Writer 失败", pw.partitionId, e);
            }
        }
        log.info("WalFileWriter 已安全关闭（未结束窗口保留在 {} 目录，下次启动自动恢复/定稿）", STAGING_DIR);
    }

    /**
     * 启动恢复：staging 中窗口已结束的残留文件原子改名为正式 .wal（补交给网闸/三区）；
     * 窗口尚未结束的保留原位，由新句柄以 APPEND 方式续写。
     */
    private void recoverStagingFiles() {
        try (Stream<Path> files = Files.list(stagingDirPath)) {
            files.filter(Files::isRegularFile).forEach(tmp -> {
                Long windowStart = parseWindowStart(tmp.getFileName().toString());
                if (windowStart == null) {
                    return;
                }
                if (System.currentTimeMillis() >= windowStart + windowMs) {
                    try {
                        publish(tmp, readyDirPath.resolve(tmp.getFileName()));
                        log.info("恢复并定稿历史窗口文件: {}", tmp.getFileName());
                    } catch (IOException e) {
                        log.error("恢复残留文件失败: {}", tmp, e);
                    }
                }
            });
        } catch (IOException e) {
            log.error("扫描 staging 目录失败", e);
        }
    }

    /** staging -> 输出目录的原子改名，文件系统不支持时退化为普通移动 */
    private void publish(Path tmp, Path target) throws IOException {
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /** 从 wal-yyyyMMdd-HHmm-pN.wal 中解析窗口起始时间(epoch 毫秒)，无法解析返回 null */
    private Long parseWindowStart(String fileName) {
        try {
            if (!fileName.startsWith(WAL_FILE_PREFIX) || !fileName.endsWith(WAL_SUFFIX)) {
                return null;
            }
            String middle = fileName.substring(WAL_FILE_PREFIX.length(),
                    fileName.length() - WAL_SUFFIX.length());
            if (middle.length() < 13) {
                return null;
            }
            return FILE_FMT.parse(middle.substring(0, 13), Instant::from).toEpochMilli();
        } catch (DateTimeParseException e) {
            log.warn("无法解析文件名时间: {}", fileName);
            return null;
        }
    }

    // ==================== 分区级写入器 ====================
    private class PartitionWriter {
        private final int partitionId;
        private final ConcurrentHashMap<Long, WindowFile> windows = new ConcurrentHashMap<>();

        PartitionWriter(int partitionId) {
            this.partitionId = partitionId;
        }

        void append(String message, long windowStart) {
            WindowFile wf = windows.computeIfAbsent(windowStart, this::newWindowFile);
            if (!wf.tryAppend(message)) {
                // 极端竞态：消息在窗口边界前一刻到达、定稿线程却先完成了关闭。
                // 不重开已定稿文件（三区可能已入库），晚到消息归入下一窗口。
                long nextWindow = windowStart + windowMs;
                windows.computeIfAbsent(nextWindow, this::newWindowFile).tryAppend(message);
                log.warn("分区 {} 窗口 {} 刚定稿，晚到消息已归入下一窗口文件",
                        partitionId, FILE_FMT.format(Instant.ofEpochMilli(windowStart)));
            }
        }

        void tick(long now) {
            // 先刷盘所有窗口（含当前窗口）
            for (WindowFile wf : windows.values()) {
                wf.flushBuffer();
            }
            // 再定稿所有已结束窗口
            for (Long ws : new ArrayList<>(windows.keySet())) {
                if (now >= ws + windowMs) {
                    WindowFile wf = windows.remove(ws);
                    if (wf != null) {
                        wf.finalizeFile();
                    }
                }
            }
        }

        void shutdown(long now) {
            for (Long ws : new ArrayList<>(windows.keySet())) {
                WindowFile wf = windows.remove(ws);
                if (wf == null) {
                    continue;
                }
                wf.flushAndClose();
                // 仅定稿已结束窗口；未结束窗口留在 staging，避免半成品被网闸摆渡
                if (now >= ws + windowMs) {
                    try {
                        wf.publish();
                    } catch (IOException e) {
                        log.error("分区 {} 定稿文件失败: {}", partitionId, wf.fileName, e);
                    }
                }
            }
        }

        private WindowFile newWindowFile(long windowStart) {
            String name = String.format("%s%s-p%d%s", WAL_FILE_PREFIX,
                    FILE_FMT.format(Instant.ofEpochMilli(windowStart)), partitionId, WAL_SUFFIX);
            return new WindowFile(name);
        }
    }

    // ==================== 单个(分区 × 窗口)文件 ====================
    private class WindowFile {
        private final String fileName;
        private final Path tmpPath;
        private final Path finalPath;

        private final List<String> buffer = new ArrayList<>();
        private final Object lock = new Object();
        private BufferedWriter writer;
        private volatile boolean finalized;

        WindowFile(String fileName) {
            this.fileName = fileName;
            this.tmpPath = stagingDirPath.resolve(fileName);
            this.finalPath = readyDirPath.resolve(fileName);
        }

        /** @return true 已追加；false 表示窗口已定稿，调用方应改投下一窗口 */
        boolean tryAppend(String message) {
            synchronized (lock) {
                if (finalized) {
                    return false;
                }
                buffer.add(message);
                if (buffer.size() >= BATCH_SIZE) {
                    flushBufferLocked();
                }
                return true;
            }
        }

        void flushBuffer() {
            synchronized (lock) {
                flushBufferLocked();
            }
        }

        private void flushBufferLocked() {
            if (buffer.isEmpty()) {
                return;
            }
            try {
                if (writer == null) {
                    // CREATE + APPEND：进程在窗口内重启后可续写 staging 中的残留临时文件
                    writer = Files.newBufferedWriter(tmpPath, StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
                for (String msg : buffer) {
                    writer.write(msg);
                    writer.newLine();
                }
                writer.flush();
                buffer.clear();
            } catch (IOException e) {
                log.error("写入临时文件失败: {}", tmpPath, e);
                throw new RuntimeException("WAL临时文件写入失败", e);
            }
        }

        /**
         * 窗口结束定稿：flush → close → 原子改名为正式 .wal。
         * 改名后该文件立即对网闸可见，且内容不再变化。
         */
        void finalizeFile() {
            synchronized (lock) {
                if (finalized) {
                    return;
                }
                flushBufferLocked();
                closeWriterLocked();
                finalized = true;
            }
            // 改名放在锁外执行，避免 IO 阻塞同窗口写入（定稿后写入会走下一窗口兜底）
            try {
                WalFileWriter.this.publish(tmpPath, finalPath);
                log.info("窗口文件已定稿: {}", fileName);
            } catch (IOException e) {
                log.error("文件定稿改名失败: {}", fileName, e);
            }
        }

        /** 关闭时使用：flush 并关闭句柄，不做改名 */
        void flushAndClose() {
            synchronized (lock) {
                flushBufferLocked();
                closeWriterLocked();
                finalized = true;
            }
        }

        void publish() throws IOException {
            WalFileWriter.this.publish(tmpPath, finalPath);
        }

        private void closeWriterLocked() {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    log.error("关闭临时文件失败: {}", tmpPath, e);
                }
                writer = null;
            }
        }
    }
}
