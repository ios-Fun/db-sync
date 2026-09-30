package com.changgeng.file;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.*;
import java.sql.Connection;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * WAL 文件摆渡调度服务（适配一区 WalFileWriter 输出格式 + 网闸覆盖模式）
 *
 * 一区 WalFileWriter 产生的文件名格式：wal-yyyyMMdd-HHmm-pN.wal
 * - 5 分钟一个时间窗口，每个 Kafka 分区一个文件
 * - 文件内容每行一条 CDC JSON 消息
 *
 * 三区处理策略：
 * 1. 扫描 incoming 目录中的 .wal 文件
 * 2. 通过文件名时间窗口判断文件是否已写完（跳过当前窗口正在写入的文件）
 * 3. 拷贝到 work 目录 → 单事务批量入库 → 标记已消费 → 清理副本
 *
 * ⚠️ 前置条件：
 * 1. 启动类需添加 @EnableScheduling 注解
 * 2. incoming 目录由网闸写入，应用仅有读权限
 * 3. work 目录和 .consumed_files 文件归应用用户所有
 */
@Service
@Slf4j
public class FileDispatchService {

    // ==================== 路径配置（可配置化） ====================
    @Value("${wal.receive.incoming-dir:/opt/cdc-receive/data/incoming}")
    private String incomingDir;

    @Value("${wal.receive.work-dir:/opt/cdc-receive/data/work}")
    private String workDir;

    @Value("${wal.receive.consumed-registry:/opt/cdc-receive/data/.consumed_files}")
    private String consumedRegistry;

    /** 注册表最大保留行数，超出后自动清理旧记录 */
    private static final int MAX_RETAIN_LINES = 50000;

    /** 一区 WAL 文件时间窗口大小（毫秒），需与一区 WalFileWriter.WINDOW_MS 保持一致 */
    @Value("${wal.window-ms:300000}")
    private long windowMs;

    /** 文件完成的安全间隔（毫秒），当前时间超过窗口结束时间 + 此间隔才认为文件已写完 */
    @Value("${wal.completion-safety-ms:10000}")
    private long completionSafetyMs;

    /** WAL 文件名时间格式：wal-yyyyMMdd-HHmm-pN.wal */
    private static final DateTimeFormatter FILE_TIME_FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmm").withZone(ZoneId.systemDefault());

    private static final String WAL_SUFFIX = ".wal";
    private static final String WAL_FILE_PREFIX = "wal-";

    @Qualifier("dao")
    @Autowired
    private DataSource dataSource;

    @Autowired
    private FileLineHandler fileLineHandler;

    /** 内存缓存，避免每次扫描都读磁盘 */
    private final Set<String> consumedFiles = new ConcurrentSkipListSet<>();

    private Path incomingPath;
    private Path workPath;

    @PostConstruct
    public void init() throws IOException {
        incomingPath = Paths.get(incomingDir);
        workPath = Paths.get(workDir);
        Files.createDirectories(workPath);

        Path registryPath = Paths.get(consumedRegistry);
        if (Files.exists(registryPath)) {
            List<String> lines = Files.readAllLines(registryPath);
            consumedFiles.addAll(lines);
            log.info("✅ 加载已消费文件记录 {} 条", consumedFiles.size());
        } else {
            log.info("ℹ️ 消费注册表不存在，首次启动将创建");
        }

        log.info("WAL 文件调度服务初始化: incoming={}, work={}, windowMs={}, safetyMs={}",
                incomingPath.toAbsolutePath(), workPath.toAbsolutePath(), windowMs, completionSafetyMs);
    }

    /**
     * 定时扫描 incoming 目录中的 .wal 文件
     * fixedDelay 保证上一个批次处理完成后才启动下一次扫描，避免并发冲突
     */
    @Scheduled(fixedDelay = 1000)
    public void scanAndProcess() {
        if (!Files.exists(incomingPath)) {
            return;
        }

        try (Stream<Path> files = Files.list(incomingPath)) {
            List<Path> pendingFiles = files
                    .filter(p -> p.toString().endsWith(WAL_SUFFIX))
                    .filter(p -> !consumedFiles.contains(p.getFileName().toString()))
                    .filter(this::isFileCompleted)
                    .sorted()
                    .collect(Collectors.toList());

            for (Path file : pendingFiles) {
                processSingleFile(file);
            }
        } catch (IOException e) {
            log.error("❌ 扫描 incoming 目录失败", e);
        }
    }

    /**
     * 判断 WAL 文件是否已完成写入（基于文件名中的时间窗口）
     *
     * 文件名: wal-yyyyMMdd-HHmm-pN.wal
     * 窗口开始时间 = yyyyMMdd-HHmm
     * 窗口结束时间 = 窗口开始 + windowMs
     * 当前时间 >= 窗口结束 + 安全间隔 → 文件已完成
     */
    private boolean isFileCompleted(Path file) {
        String fileName = file.getFileName().toString();
        try {
            String windowStartStr = extractWindowTime(fileName);
            if (windowStartStr == null) {
                log.warn("⚠️ 无法解析文件名时间，跳过: {}", fileName);
                return false;
            }

            Instant windowStart = FILE_TIME_FMT.parse(windowStartStr, Instant::from);
            Instant completeThreshold = windowStart.plusMillis(windowMs + completionSafetyMs);
            Instant now = Instant.now();

            if (now.isBefore(completeThreshold)) {
                // 当前窗口可能仍在写入中，跳过
                return false;
            }
            return true;
        } catch (DateTimeParseException e) {
            log.warn("⚠️ 文件名时间格式无法解析: {}", fileName);
            return false;
        } catch (Exception e) {
            log.warn("⚠️ 判断文件完成状态失败: {}", fileName, e);
            return false;
        }
    }

    /**
     * 从文件名 wal-yyyyMMdd-HHmm-pN.wal 中提取时间部分 yyyyMMdd-HHmm
     */
    private String extractWindowTime(String fileName) {
        if (!fileName.startsWith(WAL_FILE_PREFIX) || !fileName.endsWith(WAL_SUFFIX)) {
            return null;
        }
        // 去掉前缀 "wal-" 和后缀 ".wal"，得到 "yyyyMMdd-HHmm-pN"
        String middle = fileName.substring(WAL_FILE_PREFIX.length(), fileName.length() - WAL_SUFFIX.length());
        // 时间部分是前 13 位 "yyyyMMdd-HHmm"
        if (middle.length() < 13) {
            return null;
        }
        return middle.substring(0, 13);
    }

    /**
     * 单文件处理全流程：拷贝 → 事务处理 → 标记消费 → 清理副本
     */
    private void processSingleFile(Path walFile) {
        String fileName = walFile.getFileName().toString();
        Path workFile = workPath.resolve(fileName);

        // 1. 拷贝到安全工作区（隔离网闸覆盖风险）
        try {
            Files.copy(walFile, workFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.error("❌ 拷贝文件到工作区失败: {}", walFile, e);
            return;
        }

        boolean allSuccess = false;
        long processedLines = 0;
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (BufferedReader reader = Files.newBufferedReader(workFile)) {
                String line;
                boolean failed = false;
                while ((line = reader.readLine()) != null) {
                    processedLines++;
                    if (!fileLineHandler.processLine(line, conn)) {
                        log.error("❌ 处理失败 [{}:{}] , 触发回滚", fileName, processedLines);
                        failed = true;
                        break;
                    }
                }
                // 未发生失败即视为成功（空文件也算成功）
                if (!failed) {
                    allSuccess = true;
                }
            }

            if (allSuccess) {
                conn.commit();
                markConsumed(fileName);
                log.info("✅ 文件处理成功: {}, 共 {} 行", fileName, processedLines);
            } else {
                conn.rollback();
                log.warn("⚠️ 文件已回滚，等待下次重试: {}", fileName);
            }
        } catch (Exception e) {
            log.error("❌ 文件处理异常，自动回滚: {}", fileName, e);
        } finally {
            // 无论成功失败，清理工作区副本
            try {
                Files.deleteIfExists(workFile);
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * 持久化已消费标记（追加写，线程安全）
     */
    private synchronized void markConsumed(String fileName) throws IOException {
        Files.write(Paths.get(consumedRegistry),
                (fileName + "\n").getBytes(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        consumedFiles.add(fileName);
    }

    /**
     * 每日凌晨2点自动清理超过上限的消费记录
     * 无需数据库，纯文本行过滤 + 原子替换
     */
    @Scheduled(cron = "0 0 2 * * ?")
    public synchronized void cleanExpiredRecords() {
        Path registryPath = Paths.get(consumedRegistry);
        if (!Files.exists(registryPath)) {
            return;
        }

        try {
            List<String> allLines = Files.readAllLines(registryPath);
            if (allLines.size() <= MAX_RETAIN_LINES) {
                return;
            }

            List<String> retained = allLines.subList(
                    allLines.size() - MAX_RETAIN_LINES, allLines.size());

            // 先写临时文件再原子 rename，防写入中途崩溃丢失数据
            Path tempPath = registryPath.resolveSibling(".consumed_files.tmp");
            Files.write(tempPath, retained,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tempPath, registryPath,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

            // 刷新内存缓存
            consumedFiles.clear();
            consumedFiles.addAll(retained);

            log.info("🧹 注册表清理完成: {} -> {} 条", allLines.size(), retained.size());
        } catch (IOException e) {
            log.error("❌ 注册表清理失败", e);
        }
    }
}
