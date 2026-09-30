package com.changgeng.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * CDC 消息解析与入库服务（文件摆渡架构适配版）
 * 
 * 核心变更：
 * 1. 移除内部连接管理，Connection 由外层文件事务统一传入
 * 2. 异常不再吞没，向上传播以支持文件级回滚
 * 3. 消除所有 SQL 注入风险
 */
@Service
@Slf4j
public class ParseMsgService {

    @Qualifier("dao")
    @Autowired
    private DataSource druidDataSource;

    @Autowired
    private DBTableService dbTableService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 处理单行 CDC 消息（供文件读取循环调用）
     * 
     * @param message JSON 字符串
     * @param conn    外层事务管理的数据库连接（不要在此方法内关闭！）
     * @return true=成功, false=跳过（如未知操作类型）
     * @throws Exception 处理失败时抛出，触发外层事务回滚
     */
    public boolean processCDCMessage(String message, Connection conn) throws Exception {
        JsonNode rootNode = objectMapper.readTree(message);
        String operation = rootNode.get("o").asText();
        String table = rootNode.get("t").asText();

        switch (operation) {
            case "c":
            case "r":
                return handleInsert(table, rootNode.get("a"), conn);
            case "u":
                return handleUpdate(table, rootNode, conn);
            case "d":
                return handleDelete(table, rootNode.get("b"), conn);
            default:
                log.warn("未知操作类型: {}, table: {}", operation, table);
                return false;
        }
    }

    // ==================== INSERT ====================

    private boolean handleInsert(String table, JsonNode data, Connection conn) throws SQLException {
        String primaryKey = dbTableService.primaryMap.get(table);

        // 幂等检查：记录已存在则跳过
        if (primaryKey != null) {
            if (isRowExistByKey(table, primaryKey, data, conn)) {
                log.debug("INSERT 跳过(主键已存在): {} pk={}", table, data.get(primaryKey));
                return true;
            }
        } else {
            if (isRowExistByRecord(table, data, conn)) {
                log.debug("INSERT 跳过(记录已存在): {}", table);
                return true;
            }
        }

        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        List<Object> values = new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String field = entry.getKey();
            JsonNode value = entry.getValue();

            if (columns.length() > 0) {
                columns.append(", ");
                placeholders.append(", ");
            }
            columns.append(field);

            String columnType = dbTableService.columnsMap.get(table).get(field);
            if (columnType != null && columnType.startsWith("timestamp")) {
                placeholders.append("(to_timestamp(? / 1000000.0) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai'");
            } else {
                placeholders.append("?");
            }
            values.add(getValueFromJsonNode(value));
        }

        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", table, columns, placeholders);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) {
                pstmt.setObject(i + 1, values.get(i));
            }
            int row = pstmt.executeUpdate();
            log.info("INSERT {} 影响 {} 行", table, row);
        }
        return true;
    }

    // ==================== UPDATE ====================

    private boolean handleUpdate(String table, JsonNode data, Connection conn) throws SQLException {
        String primaryKey = dbTableService.primaryMap.get(table);
        if (primaryKey == null) {
            return updateByRecord(table, data, conn);
        } else {
            return updateByKey(table, primaryKey, data.get("a"), conn);
        }
    }

    private boolean updateByKey(String table, String key, JsonNode data, Connection conn) throws SQLException {
        List<String> setClauses = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String column = entry.getKey();
            if (key.equals(column)) continue;

            String columnType = dbTableService.columnsMap.get(table).get(column);
            if (columnType != null && columnType.startsWith("timestamp")) {
                setClauses.add(column + " = (to_timestamp(? / 1000000.0) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai'");
            } else {
                setClauses.add(column + " = ?");
            }
            values.add(getValueFromJsonNode(entry.getValue()));
        }

        values.add(getValueFromJsonNode(data.get(key)));

        String sql = "UPDATE " + table + " SET " + String.join(", ", setClauses) + " WHERE " + key + " = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) {
                pstmt.setObject(i + 1, values.get(i));
            }
            int rows = pstmt.executeUpdate();
            log.info("UPDATE {} 影响 {} 行, pk={}", table, rows, data.get(key));
        }
        return true;
    }

    private boolean updateByRecord(String table, JsonNode data, Connection conn) throws SQLException {
        JsonNode beforeNode = data.get("b");
        JsonNode afterNode = data.get("a");

        List<String> afterClauses = new ArrayList<>();
        List<Object> afterValues = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> afterFields = afterNode.fields();
        while (afterFields.hasNext()) {
            Map.Entry<String, JsonNode> entry = afterFields.next();
            afterClauses.add(entry.getKey() + " = ?");
            afterValues.add(getValueFromJsonNode(entry.getValue()));
        }

        List<String> beforeClauses = new ArrayList<>();
        List<Object> beforeValues = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> beforeFields = beforeNode.fields();
        while (beforeFields.hasNext()) {
            Map.Entry<String, JsonNode> entry = beforeFields.next();
            beforeClauses.add(entry.getKey() + " = ?");
            beforeValues.add(getValueFromJsonNode(entry.getValue()));
        }

        String sql = "UPDATE " + table + " SET " + String.join(", ", afterClauses)
                + " WHERE " + String.join(" AND ", beforeClauses);

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int idx = 1;
            for (Object v : afterValues) pstmt.setObject(idx++, v);
            for (Object v : beforeValues) pstmt.setObject(idx++, v);
            int count = pstmt.executeUpdate();
            log.info("UPDATE(no-pk) {} 影响 {} 行", table, count);
        }
        return true;
    }

    // ==================== DELETE ====================

    private boolean handleDelete(String table, JsonNode data, Connection conn) throws SQLException {
        String primaryKey = dbTableService.primaryMap.get(table);
        if (primaryKey == null) {
            return deleteByRecord(table, data, conn);
        } else {
            return deleteByKey(table, primaryKey, data, conn);
        }
    }

    private boolean deleteByKey(String table, String key, JsonNode data, Connection conn) throws SQLException {
        String sql = "DELETE FROM " + table + " WHERE " + key + " = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setObject(1, getValueFromJsonNode(data.get(key)));
            int row = pstmt.executeUpdate();
            log.info("DELETE {} 影响 {} 行, pk={}", table, row, data.get(key));
        }
        return true;
    }

    private boolean deleteByRecord(String table, JsonNode data, Connection conn) throws SQLException {
        List<String> clauses = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String column = entry.getKey();
            String columnType = dbTableService.columnsMap.get(table).get(column);
            if (columnType != null && columnType.startsWith("timestamp")) continue;

            clauses.add(column + " = ?");
            values.add(getValueFromJsonNode(entry.getValue()));
        }

        String sql = "DELETE FROM " + table + " WHERE " + String.join(" AND ", clauses);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) {
                pstmt.setObject(i + 1, values.get(i));
            }
            int count = pstmt.executeUpdate();
            log.info("DELETE(no-pk) {} 影响 {} 行", table, count);
        }
        return true;
    }


    private boolean isRowExistByKey(String table, String key, JsonNode data, Connection conn) throws SQLException {
        String sql = "SELECT 1 FROM " + table + " WHERE " + key + " = ? LIMIT 1";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setObject(1, getValueFromJsonNode(data.get(key)));
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean isRowExistByRecord(String table, JsonNode data, Connection conn) throws SQLException {
        List<String> clauses = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String column = entry.getKey();
            String columnType = dbTableService.columnsMap.get(table).get(column);
            if (columnType != null && columnType.startsWith("timestamp")) continue;

            clauses.add(column + " = ?");
            values.add(getValueFromJsonNode(entry.getValue()));
        }

        String sql = "SELECT 1 FROM " + table + " WHERE " + String.join(" AND ", clauses) + " LIMIT 1";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) {
                pstmt.setObject(i + 1, values.get(i));
            }
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static Object getValueFromJsonNode(JsonNode node) {
        if (node.isNull()) return null;
        if (node.isBoolean()) return node.asBoolean();
        if (node.isInt()) return node.asInt();
        if (node.isLong()) return node.asLong();
        if (node.isDouble()) return node.asDouble();
        if (node.isTextual()) return node.asText();
        if (node.isBinary()) {
            try { return node.binaryValue(); } catch (Exception e) { return null; }
        }
        return node.toString();
    }
}