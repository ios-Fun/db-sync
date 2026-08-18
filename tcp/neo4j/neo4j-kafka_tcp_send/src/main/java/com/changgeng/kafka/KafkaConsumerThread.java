package com.changgeng.kafka;

import com.changgeng.proto.SqlInfoNeo4j;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.changgeng.netty.NettyClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.gson.Gson;

@Slf4j
public class KafkaConsumerThread implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private volatile boolean isRunning = true;

    private NettyClient nettyClient;

    // 发给server的消息个数
    public AtomicInteger produceIndex = new AtomicInteger(0);

    // 收到应答的消息个数
    public AtomicInteger receiveIndex = new AtomicInteger(0);

    // 上一次的处理失败的消息
    private String lastNettyMsg = null;
    private long lastNettyTime = 0;

    // 构造方法接收主题数组
    public KafkaConsumerThread(String bootstrapServers, String groupId, String[] topics,  String autoOffsetReset, NettyClient nettyClient) {
        // 配置Kafka消费者属性
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset); // 从最早的消息开始消费
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        this.consumer = new KafkaConsumer<>(props);
        this.topics = Arrays.asList(topics); // 将数组转换为List
        this.nettyClient = nettyClient;
    }

    private void kafkaCommit() {
        log.info("kafkaCommit");
        consumer.commitAsync();
        receiveIndex.set(0);
        lastNettyMsg = null;
        lastNettyTime = 0;
    }

    @Override
    public void run() {
        try {
            // 订阅多个主题
            consumer.subscribe(topics);
            log.info("开始监听Kafka主题: " + topics);

            // 持续消费消息，直到被停止
            while (isRunning) {
                // 收到server应答后，kafka提交commit
                if (receiveIndex.get() > 0 ) {
                    if (this.consumer != null) {
                        this.kafkaCommit();
                    }
                }
                // 已发送消息数为0，可以从kafka拉取新消息
                if (produceIndex.get() <= 0 ) {
                    // 是否有上一次未成功的消息
                    if (lastNettyMsg != null) {
                        Thread.sleep(10000);
                        processMessage(lastNettyMsg);
                    }else {
                        // 从kafka拉取新的消息，拉取消息，超时时间设置为1秒
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                        for (ConsumerRecord<String, String> record : records) {
                            // 处理接收到的消息，显示具体来自哪个主题
                            log.info("收到消息: topic = {}, partition = {}, offset = {}, key = {}, value = {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                            // 在这里可以添加自己的消息处理逻辑
                            if (record.value() == null) {
                                this.kafkaCommit();
                                continue;
                            }
                            processMessage(record.value());
                            lastNettyMsg = record.value();
                            lastNettyTime = System.currentTimeMillis();
                            produceIndex.set(1);
                        }
                    }
                }else {
                    long currentTimeMillis = System.currentTimeMillis();
                    // 超时处理，如果超时20s没有收到应答
                    if (lastNettyTime != 0 && (currentTimeMillis - lastNettyTime) > (long)20000) {
                        produceIndex.set(0);
                        receiveIndex.set(0);
                        lastNettyTime = currentTimeMillis;
                    }
                }

            }
        } catch (Exception e) {
            log.error("消费消息时发生错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭消费者，释放资源
            consumer.close();
            log.info("Kafka消费者已关闭");
        }
    }

    // kafka消息处理方法，可以根据实际需求重写
    private void processMessage(String value) {
        // 这里添加消息处理逻辑
        SqlInfoNeo4j.Sql sql = parseTransaction(value.strip());
        log.info("生成的Cypher语句: {}", sql);
        if(sql != null) {
            nettyClient.sendSyncMsg(sql,this);
        }
    }

    // 停止消费者线程
    public void stop() {
        isRunning = false;
    }

    // 提交commit - 外部线程调用
    public void commitAsync(boolean isSuccess) {
        if (isSuccess) {
            this.receiveIndex.set(1);
        }
        if (this.produceIndex.get() > 0) {
            this.produceIndex.set(0);
        }
    }

    private SqlInfoNeo4j.Sql parseTransaction(String jsonMsg) {
        try {
            // 解析JSON消息
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(jsonMsg);
            // 提取事件基本信息
            JsonNode eventNode = rootNode.get("payload").get("event");
            String operation = eventNode.get("operation").asText();


            String sqlStr = "";

            if ("CREATE".equals(operation)) {
                if ("NODE".equals(eventNode.get("eventType").asText())) {
                    sqlStr = generateCreateNodeCypher(eventNode);
                }else if ("RELATIONSHIP".equals(eventNode.get("eventType").asText())) {
                    sqlStr = generateCreateRelationshipCypher(eventNode);
                }else {
                    log.warn("parseTransaction: {}", jsonMsg);
                }
            }else if ("DELETE".equals(operation)) {
                if ("NODE".equals(eventNode.get("eventType").asText())) {
                    sqlStr = generateDeleteNodeCypher(eventNode);
                }else if ("RELATIONSHIP".equals(eventNode.get("eventType").asText())) {
                    sqlStr = generateDeleteRelationshipCypher(eventNode);
                }else {
                    log.warn("parseTransaction: {}", jsonMsg);
                }
            }else if ("UPDATE".equals(operation)) {
                if ("NODE".equals(eventNode.get("eventType").asText())) {
                    sqlStr = generateUpdateNodeCypher(eventNode);
                }else if ("RELATIONSHIP".equals(eventNode.get("eventType").asText())) {
                    sqlStr = generateUpdateRelationshipCypher(eventNode);
                }else {
                    log.warn("parseTransaction: {}", jsonMsg);
                }
            }else {
                log.warn("parseTransaction: {}", jsonMsg);
            }
            if (sqlStr.length() == 0) {
                log.warn("sqlStr: {}", jsonMsg);
                return null;
            }

            SqlInfoNeo4j.Sql sql = SqlInfoNeo4j.Sql.newBuilder().setSqlStr(sqlStr)
                    .setOperation(operation).setEventType(eventNode.get("eventType").asText())
                    .setElementId(eventNode.path("elementId").asText()).build();
            return sql;
        } catch (Exception e) {
            log.error("处理消息时发生错误: {}, {}", jsonMsg, e.getMessage());
            return null;
        }
    }

    // 构建关系的更新sql
    private String generateUpdateRelationshipCypher(JsonNode eventNode) {
        StringBuilder cypher = new StringBuilder();

        // 获取关系ID
        String relationshipId = eventNode.path("elementId").asText();
        if (relationshipId == null || relationshipId.isEmpty()) {
            return "";
        }

        // 获取关系类型
        String relationshipType = eventNode.path("type").asText();

        // 获取起始节点信息
        JsonNode startNode = eventNode.path("start");
        String startNodeId = startNode.path("elementId").asText();
        if (startNodeId == null || startNodeId.isEmpty()) {
            return "";
        }

        // 获取结束节点信息
        JsonNode endNode = eventNode.path("end");
        String endNodeId = endNode.path("elementId").asText();
        if (endNodeId == null || endNodeId.isEmpty()) {
            return "";
        }

        // 获取起始节点标签
        StringBuilder startLabels = new StringBuilder();
        JsonNode startLabelsNode = startNode.path("labels");
        if (startLabelsNode.isArray()) {
            for (JsonNode label : startLabelsNode) {
                startLabels.append(":").append(label.asText());
            }
        }

        // 获取结束节点标签
        StringBuilder endLabels = new StringBuilder();
        JsonNode endLabelsNode = endNode.path("labels");
        if (endLabelsNode.isArray()) {
            for (JsonNode label : endLabelsNode) {
                endLabels.append(":").append(label.asText());
            }
        }

        // 获取更新后的属性
        JsonNode afterNode = eventNode.path("state").path("after");
        if (afterNode.isMissingNode()) {
            return "";
        }

        // 构建更新属性（使用更新后的值）
        JsonNode afterPropertiesNode = afterNode.path("properties");
        StringBuilder setProperties = new StringBuilder();
        if (!afterPropertiesNode.isMissingNode()) {
            setProperties = buildPropertiesString(afterPropertiesNode);
        }

        // 构建更新关系的Cypher语句
        cypher.append("MATCH (a").append(startLabels).append(")-[r:").append(relationshipType).append("]->(b").append(endLabels).append(") ");
        cypher.append("WHERE a.id = '").append(escapeCypher(startNodeId)).append("' ");
        cypher.append("AND b.id = '").append(escapeCypher(endNodeId)).append("' ");
        // cypher.append("AND r.id = '").append(escapeCypher(relationshipId)).append("' ");
        cypher.append("SET r.").append(setProperties);

        return cypher.toString();
    }

    /**
     * 　node的更新
     * @param eventNode
     * @return
     */
    private String generateUpdateNodeCypher(JsonNode eventNode) {
        StringBuilder cypher = new StringBuilder();

        // 获取elementId作为更新依据
        String elementId = eventNode.path("elementId").asText();
        if (elementId == null || elementId.isEmpty()) {
            return "";
        }

        // 获取标签信息（可选，用于更精确的匹配）
        StringBuilder labels = new StringBuilder();
        JsonNode labelsNode = eventNode.path("labels");
        if (labelsNode.isArray()) {
            for (JsonNode label : labelsNode) {
                labels.append(":").append(label.asText());
            }
        }

        // 获取更新后的属性用于设置新值
        JsonNode afterNode = eventNode.path("state").path("after");
        if (afterNode.isMissingNode()) {
            return "";
        }

        // 构建更新属性（使用更新后的值）
        JsonNode afterPropertiesNode = afterNode.path("properties");
        StringBuilder setProperties = new StringBuilder();
        if (!afterPropertiesNode.isMissingNode()) {
            setProperties = buildNodePropertiesString(afterPropertiesNode);
        }

        // 构建更新节点的Cypher语句，使用elementId作为匹配条件
        cypher.append("MATCH (n").append(labels).append(") ");
        // cypher.append("WHERE n.id = '").append(escapeCypher(elementId)).append("' ");
        cypher.append("WHERE n.id= '").append(escapeCypher(elementId)).append("' or elementId(n) = '").append(escapeCypher(elementId)).append("' ");
        cypher.append("SET ").append(setProperties);

        return cypher.toString();
    }

    // 关系的删除sql
    private String generateDeleteRelationshipCypher(JsonNode eventNode) {
        StringBuilder cypher = new StringBuilder();

        // 获取关系ID
        String relationshipId = eventNode.path("elementId").asText();
        if (relationshipId == null || relationshipId.isEmpty()) {
            return "";
        }

        // 获取关系类型
        String relationshipType = eventNode.path("type").asText();

        // 获取起始节点信息
        JsonNode startNode = eventNode.path("start");
        String startNodeId = startNode.path("elementId").asText();
        if (startNodeId == null || startNodeId.isEmpty()) {
            return "";
        }

        // 获取结束节点信息
        JsonNode endNode = eventNode.path("end");
        String endNodeId = endNode.path("elementId").asText();
        if (endNodeId == null || endNodeId.isEmpty()) {
            return "";
        }

        // 获取起始节点标签
        StringBuilder startLabels = new StringBuilder();
        JsonNode startLabelsNode = startNode.path("labels");
        if (startLabelsNode.isArray()) {
            for (JsonNode label : startLabelsNode) {
                startLabels.append(":").append(label.asText());
            }
        }

        // 获取结束节点标签
        StringBuilder endLabels = new StringBuilder();
        JsonNode endLabelsNode = endNode.path("labels");
        if (endLabelsNode.isArray()) {
            for (JsonNode label : endLabelsNode) {
                endLabels.append(":").append(label.asText());
            }
        }

        // 构建删除关系的Cypher语句
        cypher.append("MATCH (a").append(startLabels).append(")-[r:").append(relationshipType).append("]->(b").append(endLabels).append(") ");
        //cypher.append("WHERE a.id = '").append(escapeCypher(startNodeId)).append("' ");
        cypher.append("WHERE (a.id= '").append(escapeCypher(startNodeId)).append("' or elementId(a) = '").append(escapeCypher(startNodeId)).append("') ");
        cypher.append("AND (b.id= '").append(escapeCypher(endNodeId)).append("' or elementId(b) = '").append(escapeCypher(endNodeId)).append("') ");
        // cypher.append("AND b.id = '").append(escapeCypher(endNodeId)).append("' ");
        // cypher.append("AND r.id = '").append(escapeCypher(relationshipId)).append("' ");
        cypher.append("DELETE r");

        return cypher.toString();
    }

    /**
     * node的删除
     * @param eventNode
     * @return
     */
    private String generateDeleteNodeCypher(JsonNode eventNode) {
        StringBuilder cypher = new StringBuilder();

        // 获取elementId作为删除依据
        String elementId = eventNode.path("elementId").asText();
        if (elementId == null || elementId.isEmpty()) {
            return "";
        }

        // 获取标签信息（可选，用于更精确的匹配）
        StringBuilder labels = new StringBuilder();
        JsonNode labelsNode = eventNode.path("labels");
        if (labelsNode.isArray()) {
            for (JsonNode label : labelsNode) {
                labels.append(":").append(label.asText());
            }
        }

        // 构建删除节点的Cypher语句，使用elementId
        // 在Neo4j中，elementId是内部标识符，可以通过elementId()函数匹配
        cypher.append("MATCH (n").append(labels).append(") ");
        cypher.append("WHERE n.id= '").append(escapeCypher(elementId)).append("' or elementId(n) = '").append(escapeCypher(elementId)).append("' ");
        cypher.append("DELETE n");

        return cypher.toString();
    }

    // 构建生成关系的sql
    private String generateCreateRelationshipCypher(JsonNode eventNode) {
        StringBuilder cypher = new StringBuilder();

        // 获取关系的基本信息
        String relationshipType = eventNode.path("type").asText();
        String relationshipId = eventNode.path("elementId").asText();

        // 获取起始节点信息
        JsonNode startNode = eventNode.path("start");
        String startNodeId = startNode.path("elementId").asText();
        if (startNodeId == null || startNodeId.isEmpty()) {
            return "";
        }

        // 获取结束节点信息
        JsonNode endNode = eventNode.path("end");
        String endNodeId = endNode.path("elementId").asText();
        if (endNodeId == null || endNodeId.isEmpty()) {
            return "";
        }

        // 获取起始节点标签
        StringBuilder startLabels = new StringBuilder();
        JsonNode startLabelsNode = startNode.path("labels");
        if (startLabelsNode.isArray()) {
            for (JsonNode label : startLabelsNode) {
                startLabels.append(":").append(label.asText());
            }
        }

        // 获取结束节点标签
        StringBuilder endLabels = new StringBuilder();
        JsonNode endLabelsNode = endNode.path("labels");
        if (endLabelsNode.isArray()) {
            for (JsonNode label : endLabelsNode) {
                endLabels.append(":").append(label.asText());
            }
        }

        // 获取关系属性
        JsonNode afterNode = eventNode.path("state").path("after");
        if (afterNode.isMissingNode()) {
            return "";
        }

        // 构建关系属性
        JsonNode propertiesNode = afterNode.path("properties");
        StringBuilder relationshipProperties = new StringBuilder();
        if (!propertiesNode.isMissingNode()) {
            relationshipProperties = buildCreatePropertiesString(propertiesNode, eventNode.get("elementId").asText());
            if (relationshipProperties.length() > 0) {
                relationshipProperties.insert(0, " {").append("}");
            }
        }

        // 构建创建关系的Cypher语句
        cypher.append("MATCH (a").append(startLabels).append("), (b").append(endLabels).append(") ");
        cypher.append("WHERE a.id = '").append(escapeCypher(startNodeId)).append("' ");
        cypher.append("AND b.id = '").append(escapeCypher(endNodeId)).append("' ");
        cypher.append("CREATE (a)-[r:").append(relationshipType).append(relationshipProperties).append("]->(b)");

        return cypher.toString();
    }

    // 节点创建sql
    private String generateCreateNodeCypher(JsonNode eventNode) {
        StringBuilder cypher = new StringBuilder();
        cypher.append("CREATE (");

        // 添加节点变量，使用第一个标签的小写形式
        String varName = eventNode.get("labels").get(0).asText().toLowerCase();
        cypher.append(varName);

        // 添加所有标签
        JsonNode labelsNode = eventNode.get("labels");
        for (int i = 0; i < labelsNode.size(); i++) {
            cypher.append(":").append(labelsNode.get(i).asText());
        }

        // 添加属性
        cypher.append(" { ");

        // 添加elementId作为id属性
        cypher.append("id: '").append(escapeCypher(eventNode.get("elementId").asText())).append("', ");

        // 处理所有其他属性
        JsonNode propertiesNode = eventNode.get("state").get("after").get("properties");
        Iterator<Map.Entry<String, JsonNode>> fields = propertiesNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode valueNode = field.getValue();

            // 添加属性名
            cypher.append(fieldName).append(": ");

            // 处理不同类型的值
            String value = getValueAsCypher(valueNode);
            cypher.append(value);

            if (fields.hasNext()) {
                cypher.append(", ");
            }
        }
        cypher.append(" })");
        return cypher.toString();
    }

    private StringBuilder buildNodePropertiesString(JsonNode propertiesNode) {
        StringBuilder properties = new StringBuilder();

        Iterator<Map.Entry<String, JsonNode>> fields = propertiesNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String propName = field.getKey();
            JsonNode propValueNode = field.getValue();

            // 解析属性值
            String propValue = getValueAsCypher(propValueNode);
            if (propValue != null) {
                if (properties.length() > 0) {
                    properties.append(", ");
                }
                // properties.append(propName).append(": ").append(propValue);
                properties.append("n.").append(propName).append("= ").append(propValue);
            }
        }

        return properties;
    }

    private StringBuilder buildCreatePropertiesString(JsonNode propertiesNode,String elementId) {
        StringBuilder properties = new StringBuilder();

        Iterator<Map.Entry<String, JsonNode>> fields = propertiesNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String propName = field.getKey();
            JsonNode propValueNode = field.getValue();

            // 解析属性值
            String propValue = getValueAsCypher(propValueNode);
            if (propValue != null) {
                if (properties.length() > 0) {
                    properties.append(", ");
                }
                properties.append(propName).append(": ").append(propValue);
            }
        }
        properties.append(", id").append(": '").append(elementId).append("'");

        return properties;
    }

    private StringBuilder buildPropertiesString(JsonNode propertiesNode) {
        StringBuilder properties = new StringBuilder();

        Iterator<Map.Entry<String, JsonNode>> fields = propertiesNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String propName = field.getKey();
            JsonNode propValueNode = field.getValue();

            // 解析属性值
            String propValue = getValueAsCypher(propValueNode);
            if (propValue != null) {
                if (properties.length() > 0) {
                    properties.append(", ");
                }
                // properties.append(propName).append(": ").append(propValue);
                properties.append(propName).append("= ").append(propValue);
            }
        }

        return properties;
    }

    private String getValueAsCypher(JsonNode valueNode) {
        String type = valueNode.get("type").asText();

        switch (type) {
            case "S":
                // 字符串类型需要用单引号括起来
                return "'" + escapeCypher(valueNode.get("S").asText()) + "'";
            case "I64":
            case "F64":
                // 数字类型不需要引号
                return valueNode.get(type).asText();
            case "TZDT":
                // 日期时间类型在Cypher中作为字符串处理
                return "'" + escapeCypher(valueNode.get("TZDT").asText()) + "'";
            default:
                return "'" + escapeCypher(valueNode.toString()) + "'";
        }
    }

    // 转义Cypher中的特殊字符，特别是单引号
    private String escapeCypher(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("'", "\\'");
    }
}
