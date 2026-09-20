package com.changgeng.service;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 获取表的主键信息
 * @author wangyouzhuo
 */
@Component
@Slf4j
public class DBTableService {
    @Qualifier("dao")
    @Autowired
    DataSource druidDataSource;

    @Value("${db.schema}")
    String schema;

    // 表对应的主键map
    Map<String, String> primaryMap = new HashMap();

    // 所有的列的类型
    Map<String, Map<String, String>> columnsMap = new HashMap<>();

    public void getPrimaryKey() {
        log.info("getPrimaryKey");
        String sql = String.format("SELECT    tc.table_name, kcu.column_name FROM information_schema.table_constraints AS tc JOIN information_schema.constraint_column_usage AS kcu \n" +
                        "ON tc.constraint_name = kcu.constraint_name\n" +
                        "WHERE tc.constraint_type = 'PRIMARY KEY' and tc.table_schema = '%s' ORDER BY tc.table_name", schema);

        Connection conn = null;
        try  {
            conn = druidDataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String columnName = rs.getString("column_name");
                primaryMap.put(tableName, columnName);
            }
            log.info("getPrimaryKey: {}", primaryMap.size());
        } catch (Exception e) {
            log.error("getPrimaryKey: {}", e.getMessage());
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

    // 获取所有列的类型
    public void getAllColumn() {
        String sql = "SELECT\n" +
                "    t.table_name,\n" +
                "    c.column_name,\n" +
                "    c.data_type\n" +
                "FROM information_schema.tables t\n" +
                "LEFT JOIN pg_description d1\n" +
                "    ON d1.objoid = (SELECT oid FROM pg_class WHERE relname = t.table_name AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = t.table_schema))\n" +
                "    AND d1.objsubid = 0\n" +
                "LEFT JOIN information_schema.columns c\n" +
                "    ON c.table_schema = t.table_schema AND c.table_name = t.table_name\n" +
                "LEFT JOIN pg_description d2\n" +
                "    ON d2.objoid = (SELECT oid FROM pg_class WHERE relname = c.table_name AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = c.table_schema))\n" +
                "    AND d2.objsubid = c.ordinal_position\n" +
                "WHERE t.table_schema = 'public'\n" +
                "  AND t.table_type = 'BASE TABLE'\n" +
                "ORDER BY t.table_name, c.ordinal_position;";
        Connection conn = null;
        try  {
            conn = druidDataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString("table_name");
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                if (!columnsMap.containsKey(tableName)) {
                    columnsMap.put(tableName, new HashMap<>());
                }
                Map<String,String> tableColumnMap = columnsMap.get(tableName);
                tableColumnMap.put(columnName, dataType);
            }
            log.info("getAllColumn: {}", columnsMap.size());
        } catch (Exception e) {
            log.error("getAllColumn: {}", e.getMessage());
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
}
