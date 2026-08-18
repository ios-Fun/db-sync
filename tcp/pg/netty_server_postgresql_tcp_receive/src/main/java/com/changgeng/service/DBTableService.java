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

    Map<String, String> primaryMap = new HashMap();

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
}
