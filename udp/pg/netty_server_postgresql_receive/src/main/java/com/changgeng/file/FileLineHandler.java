package com.changgeng.file;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.sql.Connection;
import com.changgeng.service.ParseMsgService;

/**
 * 接收单行 JSON 字符串，调用 ParseMsgService 完成解析与入库
 */
@Service
public class FileLineHandler {

    private static final Logger log = LoggerFactory.getLogger(FileLineHandler.class);

    @Autowired
    private ParseMsgService parseMsgService;
    
    /**
     * 处理单行 CDC 数据
     * @param line 从 .ready 文件中读取的一行 JSON 字符串
     * @return true=处理成功, false=处理失败（用于上层决定是否回滚事务）
     */
    // FileLineHandler.java 中的调用方式变更
    public boolean processLine(String line, Connection conn) {
        if (line == null || line.trim().isEmpty()) return true;
        try {
            parseMsgService.processCDCMessage(line, conn);  // ← 传入外层连接
            return true;
        } catch (Exception e) {
            log.error("处理文件行失败: {}", line, e);
            return false;  // ← 返回false触发外层事务回滚
        }
    }
}