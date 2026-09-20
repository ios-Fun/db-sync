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
 * 解析kafka消息，并添加至数据库
 */
@Service
@Slf4j
public class ParseMsgService {

    @Qualifier("dao")
    @Autowired
    DataSource druidDataSource;
    ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    DBTableService dbTableService;

    /**
     * 处理Debezium格式的消息
     */
    public boolean processCDCMessage(String message) {
        try {
            JsonNode rootNode = objectMapper.readTree(message);

            // 获取操作类型 (c: create, u: update, d: delete, r: read)
            String operation = rootNode.get("o").asText();

            // 获取表名
            String source = rootNode.get("t").asText();

            // 根据操作类型处理数据
            switch (operation) {
                case "c":  // 创建操作
                    return handleInsert(source, rootNode.get("a"));
                case "u":  // 更新操作
                    return handleUpdate(source, rootNode);
                case "d":  // 删除操作
                    return handleDelete(source, rootNode.get("b"));
                case "r":  // 读取操作（初始快照）
                    return handleInsert(source, rootNode.get("a"));
                default:
                    log.warn("未知操作类型: " + operation);
            }

            // 提交事务
            // targetDbConnection.commit();
            return true;

        } catch (Exception e) {
            log.error("处理Debezium消息失败: " + e.getMessage());
            return true;
        }
    }

    /**
     * 根据主键来判断记录是否已经存在
     */
    private boolean isRowExistByKey(String table, String key, JsonNode data) {
        String keyValue = data.get(key).asText();
        String sql = String.format("select * from %s where %s = '%s' limit 1", table, key, keyValue);
        Connection conn = null;
        try  {
            conn = druidDataSource.getConnection();
            Statement stmt = conn.createStatement();
            log.info("isRowExistByKey sql: {}", sql);
            ResultSet rs = stmt.executeQuery(sql);
            boolean hasRecord = false;
            while (rs.next()) {
                hasRecord = true;
                break;
            }
            log.info("isRowExistByKey record: {}", hasRecord);
            return hasRecord;
        } catch (Exception e) {
            log.error("getPrimaryKey1: {}", e.getMessage());
            return false;
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("isNodeExist2:{}", e.getMessage());
                }
            }
        }
    }

    /**
     * 根据数据记录判断是否已经存在
     * @param table
     * @param data
     * @return
     */
    private boolean isRowExistByRecord(String table, JsonNode data) {
        List<String> setClauses = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> dataFields = data.fields();
        while (dataFields.hasNext()) {
            Map.Entry<String, JsonNode> field = dataFields.next();
            String column = field.getKey();

            JsonNode valueNode = field.getValue();

            String columnType = dbTableService.columnsMap.get(table).get(column);
            // 去掉时间列
            if (columnType.startsWith("timestamp")) {
                continue;
            }
            setClauses.add(column + " = ?");
            values.add(getValueFromJsonNode(valueNode));
        }

        // 构建SQL，不使用主键
        String sql = "select * from " + table + " where " +
                String.join(" and ", setClauses) +
                " limit 1";

        // executeSql(sql, values);
        Connection conn = null;
        try {
            conn = druidDataSource.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < values.size(); i++) {
                pstmt.setObject(i+1, values.get(i));
            }
            log.info("isRowExistByRecord sql: {}", pstmt);
            ResultSet rs = pstmt.executeQuery();
            boolean hasRecord = false;
            while (rs.next()) {
                hasRecord = true;
                break;
            }
            log.info("isRowExistByRecord record: {}", hasRecord);
            return hasRecord;
        } catch (Exception e) {
            log.error("isRowExistByRecord1: " + e.getMessage());
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("isRowExistByRecord2:{}", e.getMessage());
                }
            }
        }
    }

    /**
     * 处理插入操作
     */
    private boolean handleInsert(String table, JsonNode data) {
        String primaryKey = dbTableService.primaryMap.get(table);
        if (primaryKey == null) {
            // 不存在主键
            boolean hasRecord = isRowExistByRecord(table, data);
            if (hasRecord) {
                log.info("isRowExistByRecord true");
                return true;
            }
        } else {
            // 存在主键，根据主键判断记录是否已经存在
            boolean hasRecord = isRowExistByKey(table, primaryKey, data);
            if (hasRecord) {
                log.info("isRowExistByKey true");
                return true;
            }
        }

        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();

        // 构建INSERT语句的列和占位符
        data.fieldNames().forEachRemaining(field -> {
            if (columns.length() > 0) {
                columns.append(", ");
                placeholders.append(", ");
            }
            columns.append(field);
            String columnType = dbTableService.columnsMap.get(table).get(field);
            if (columnType.startsWith("timestamp")) {
                placeholders.append("(to_timestamp(? / 1000000.0) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai'");
            }else {
                placeholders.append("?");
            }

        });

        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                table, columns.toString(), placeholders.toString());
        Connection conn = null;
        try  {
            conn = druidDataSource.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            int index = 1;
//            for (JsonNode value : data) {
//                setPreparedStatementValue(pstmt, index++, value);
//            }
            for (Iterator<Map.Entry<String, JsonNode>> it = data.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = it.next();
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                setPreparedStatementValue(pstmt, index++, value);
            }
            log.info("handleInsert sql: {}", pstmt);
            int row = pstmt.executeUpdate();
            log.info("已插入 {} 表{}条记录", table, row);
            return true;
        } catch (Exception e) {
            log.error("handleInsert: {}", e.getMessage());
            return false;
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("isNodeExist2:{}", e.getMessage());
                }
            }
        }
    }

    /**
     * 根据主键去删除
     * @param table
     * @param key
     * @param data
     * @return
     */
    private boolean deleteByKey(String table, String key, JsonNode data) {
        String keyValue = data.get(key).asText();
        String sql = String.format("delete from %s where %s = '%s'", table, key, keyValue);
        Connection conn = null;
        try  {
            conn = druidDataSource.getConnection();
            Statement stmt = conn.createStatement();
            log.info("deleteByKey sql: {}", sql);
            int row = stmt.executeUpdate(sql);
            log.info("deleteByKey row: {}", row);
            return true;
        } catch (Exception e) {
            log.error("deleteByKey 1: {}", e.getMessage());
            return false;
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("deleteByKey 2:{}", e.getMessage());
                }
            }
        }
    }

    /**
     * 根据主键去,更新
     * @param table
     * @param key
     * @param data
     * @return
     */
    private boolean updateByKey(String table, String key, JsonNode data) {
        List<String> setClauses = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> dataFields = data.fields();
        while (dataFields.hasNext()) {
            Map.Entry<String, JsonNode> field = dataFields.next();
            String column = field.getKey();

            // 跳过id字段，它将用作WHERE条件
            if (key.equals(column)) {
                continue;
            }

            JsonNode valueNode = field.getValue();
            String columnType = dbTableService.columnsMap.get(table).get(column);
            if (columnType.startsWith("timestamp")) {
                setClauses.add(column + " = (to_timestamp(? / 1000000.0) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Shanghai'");
            }else {
                setClauses.add(column + " = ?");
            }
            values.add(getValueFromJsonNode(valueNode));
        }

        // 获取id值并添加到参数列表
        Object keyValue = getValueFromJsonNode(data.get(key));
        values.add(keyValue);

        // 构建SQL，使用id作为条件
        String sql = "UPDATE " + table + " SET " +
                String.join(", ", setClauses) +
                String.format(" WHERE %s = ?", key);

        // executeSql(sql, values);
        Connection conn = null;
        try {
            conn = druidDataSource.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < values.size(); i++) {
                pstmt.setObject(i+1, values.get(i));
            }

            log.info("updateByKey sql: {}" ,pstmt);
            int rowsAffected = pstmt.executeUpdate();
            log.info("updateByKey row: {}", rowsAffected);
            return true;
        } catch (Exception e) {
            log.error("updateByKey 1: {}", e.getMessage());
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("updateByKey 2:{}", e.getMessage());
                }
            }
        }
    }

    /**
     * 没有主键的表，更新
     * @param table
     * @param data
     * @return
     */
    private boolean updateByRecord(String table, JsonNode data) {
        List<String> beforeSetClauses = new ArrayList<>();
        List<Object> beforeValues = new ArrayList<>();
        JsonNode beforeNode = data.get("b");
        Iterator<Map.Entry<String, JsonNode>> beforeDataFields = beforeNode.fields();
        while (beforeDataFields.hasNext()) {
            Map.Entry<String, JsonNode> field = beforeDataFields.next();
            String column = field.getKey();

            JsonNode valueNode = field.getValue();
            beforeSetClauses.add(column + " = ?");
            beforeValues.add(getValueFromJsonNode(valueNode));
        }


        List<String> afterSetClauses = new ArrayList<>();
        List<Object> afterValues = new ArrayList<>();
        JsonNode afterNode = data.get("a");
        Iterator<Map.Entry<String, JsonNode>> afterDataFields = afterNode.fields();
        while (afterDataFields.hasNext()) {
            Map.Entry<String, JsonNode> field = afterDataFields.next();
            String column = field.getKey();

            JsonNode valueNode = field.getValue();
            afterSetClauses.add(column + " = ?");
            afterValues.add(getValueFromJsonNode(valueNode));
        }
        // 构建SQL
        String sql = String.format("update %s set %s where %s", table, String.join(" , ", afterSetClauses),String.join(" and ", beforeSetClauses));

        Connection conn = null;
        try {
            conn = druidDataSource.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < afterValues.size(); i++) {
                pstmt.setObject(i+1, afterValues.get(i));
            }
            for (int j=0; j < beforeValues.size(); j++) {
                pstmt.setObject(j+afterValues.size()+1, beforeValues.get(j));
            }
            log.info("updateByRecord sql: {}", pstmt.toString());
            int count = pstmt.executeUpdate();
            log.info("updateByRecord count: {}", count);
            return true;
        } catch (Exception e) {
            log.error("updateByRecord 1: {}", e.getMessage());
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("updateByRecord 2:{}", e.getMessage());
                }
            }
        }
    }

    /**
     * 没有主键的表，删除
     * @param table
     * @param data
     * @return
     */
    private boolean deleteByRecord(String table, JsonNode data) {
        List<String> setClauses = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        Iterator<Map.Entry<String, JsonNode>> dataFields = data.fields();
        while (dataFields.hasNext()) {
            Map.Entry<String, JsonNode> field = dataFields.next();
            String column = field.getKey();

            JsonNode valueNode = field.getValue();
            String columnType = dbTableService.columnsMap.get(table).get(column);
            // 去掉时间列
            if (columnType.startsWith("timestamp")) {
                continue;
            }
            setClauses.add(column + " = ?");
            values.add(getValueFromJsonNode(valueNode));
        }

        // 构建SQL
        String sql = "delete from " + table + " where " +
                String.join(" and ", setClauses);

        // executeSql(sql, values);
        Connection conn = null;
        try {
            conn = druidDataSource.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < values.size(); i++) {
                pstmt.setObject(i+1, values.get(i));
            }
            log.info("deleteByRecord sql: {}", pstmt);
            int count = pstmt.executeUpdate();
            log.info("deleteByRecord count: {}", count);
            return true;
        } catch (Exception e) {
            log.error("deleteByRecord 1: {}", e.getMessage());
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("deleteByRecord 2:{}", e.getMessage());
                }
            }
        }
    }

    /**
     * 处理更新操作
     */
    private boolean handleUpdate(String table, JsonNode data) {
        String primaryKey = dbTableService.primaryMap.get(table);
        if (primaryKey == null) {
            // 不存在主键
            return updateByRecord(table, data);

        } else {
            // 存在主键，根据主键判断记录是否已经存在
            return updateByKey(table, primaryKey, data.get("a"));
        }

    }

    // 从JsonNode获取值
    private static Object getValueFromJsonNode(JsonNode node) {
        if (node.isNull()) {
            return null;
        } else if (node.isBoolean()) {
            return node.asBoolean();
        } else if (node.isInt()) {
            return node.asInt();
        } else if (node.isLong()) {
            return node.asLong();
        } else if (node.isDouble()) {
            return node.asDouble();
        } else if (node.isTextual()) {
            return node.asText();
        } else if (node.isBinary()) {
            // 处理二进制数据
            try {
                return node.binaryValue();
            } catch (Exception e) {
                return null;
            }
        } else {
            // 对于复杂类型，转换为字符串
            return node.toString();
        }
    }

    /**
     * 处理删除操作
     */
    private boolean handleDelete(String table, JsonNode data) {
        String primaryKey = dbTableService.primaryMap.get(table);
        if (primaryKey == null) {
            // 不存在主键
            return deleteByRecord(table, data);

        } else {
            // 存在主键，根据主键判断记录是否已经存在
            return deleteByKey(table, primaryKey, data);
        }
    }

    /**
     * 根据JSON值类型设置PreparedStatement参数
     */
    private void setPreparedStatementValue(PreparedStatement pstmt, int index, JsonNode value)
            throws SQLException {
        if (value.isNull()) {
            pstmt.setNull(index, java.sql.Types.NULL);
        } else if (value.isInt()) {
            pstmt.setInt(index, value.asInt());
        } else if (value.isLong()) {
            pstmt.setLong(index, value.asLong());
        } else if (value.isDouble()) {
            pstmt.setDouble(index, value.asDouble());
        } else if (value.isBoolean()) {
            pstmt.setBoolean(index, value.asBoolean());
        } else if (value.isTextual()) {
            pstmt.setString(index, value.asText());
        } else if (value.isBinary()) {
            try {
                pstmt.setBytes(index, value.binaryValue());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 对于其他类型，转换为字符串
            pstmt.setString(index, value.toString());
        }

    }
}
