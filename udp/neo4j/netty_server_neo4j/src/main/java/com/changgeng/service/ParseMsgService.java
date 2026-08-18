package com.changgeng.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;

/**
 * @author wangyouzhuo
 */
@Service
@Slf4j
public class ParseMsgService {

    @Qualifier("dao")
    @Autowired
    DataSource druidDataSource;

    private static String EQUAL = "EQUAL";

    public boolean processCDCMessage(Map<String,String> map) {

        Connection conn = null;
        try {
            String sqlStr = map.get("s");
            if (sqlStr == null || sqlStr.length() == 0) {
                return false;
            }
            String operation = map.get("o");
            if (operation == null || operation.length() == 0) {
                return false;
            }
            String eventType = map.get("e");
            if (eventType == null || eventType.length() == 0) {
                return false;
            }
            String elementId = map.get("i");
            if (elementId == null || elementId.length() == 0) {
                return false;
            }
            // 如果是新增
            if ("CREATE".equals(operation)) {
                if ("NODE".equals(eventType)) {
                    boolean exist = isNodeExist(elementId);
                    if (exist) {
                        return true;
                    }
                } else if ("RELATIONSHIP".equals(eventType)) {
                    boolean exist = isRelationshipExist(elementId);
                    if (exist) {
                        return true;
                    }
                }
            }else if ("DELETE".equals(operation)) {
                if ("NODE".equals(eventType)) {
                    boolean exist = isNodeExist(elementId);
                    if (!exist) {
                        return true;
                    }
                } else if ("RELATIONSHIP".equals(eventType)) {
                    boolean exist = isRelationshipExist(elementId);
                    if (!exist) {
                        return true;
                    }
                }
            }

            conn = druidDataSource.getConnection();
            Statement stmt = conn.createStatement();
            log.info("processCDCMessage sql: {}", sqlStr);
            ResultSet rs = stmt.executeQuery(sqlStr);
            return true;
        } catch (Exception e) {
            log.error("processCDCMessage1:{}", e.getMessage());
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("processCDCMessage2:{}", e.getMessage());
                }
            }
        }
    }

    /**
     * 判断Node是否存在
     * @param elementId
     * @return
     */
    private boolean isNodeExist(String elementId) {
        Connection conn = null;
        try {
            String sqlStr = String.format("match (n) where (elementId(n) = '%s' or n.id = '%s') return n limit 1", elementId, elementId);
            conn = druidDataSource.getConnection();
            Statement stmt = conn.createStatement();
            log.info("isNodeExist sql: {}", sqlStr);
            ResultSet rs = stmt.executeQuery(sqlStr);
            boolean flag = false;
            if (rs.next()) {
                flag = true;
            }
            log.info("isNodeExist flag: {}", flag);
            return flag;
        } catch (Exception e) {
            log.error("isNodeExist 1:{}", e.getMessage());
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("isNodeExist 2:{}", e.getMessage());
                }
            }
        }
    }

    /**
     * 判断Node是否存在
     * @param elementId
     * @return
     */
    private boolean isRelationshipExist(String elementId) {
        Connection conn = null;
        try {
            String sqlStr = String.format("match (n)-[r]->(m) where (elementId(r) = '%s' or r.id = '%s') return r limit 1", elementId, elementId);
            conn = druidDataSource.getConnection();
            Statement stmt = conn.createStatement();
            log.info("isRelationshipExist sql: {}", sqlStr);
            ResultSet rs = stmt.executeQuery(sqlStr);
            boolean flag = false;
            if (rs.next()) {
                flag = true;
            }
            log.info("isRelationshipExist flag: {}", flag);
            return flag;
        } catch (Exception e) {
            log.error("isRelationshipExist 1: {}", e.getMessage());
            return false;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.error("isRelationshipExist 2: {}", e.getMessage());
                }
            }
        }
    }
}
