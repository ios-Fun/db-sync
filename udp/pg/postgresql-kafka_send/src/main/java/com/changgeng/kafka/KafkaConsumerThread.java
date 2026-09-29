package com.changgeng.kafka;

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
import java.util.regex.Pattern;

import com.google.gson.Gson;

import static java.lang.Math.abs;

@Slf4j
public class KafkaConsumerThread implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final String topicPatten;
    private volatile boolean isRunning = true;

    private NettyClient nettyClient;

    // 发给server的消息个数
    public AtomicInteger produceIndex = new AtomicInteger(0);

    // 收到应答的消息个数
    public AtomicInteger receiveIndex = new AtomicInteger(0);

    // 上一次的处理失败的消息
    private String lastNettyMsg = null;
    private long lastNettyTime = 0;

    Boolean udpAsk = Boolean.FALSE;

    // 构造方法接收主题数组
    public KafkaConsumerThread(String bootstrapServers, String groupId, String topicPatten, String autoOffsetReset, Boolean udpAsk, NettyClient nettyClient) {
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
        this.topicPatten = topicPatten; // 将数组转换为List
        this.nettyClient = nettyClient;
        this.udpAsk = udpAsk;
    }

    private void kafkaCommit() {
        consumer.commitAsync();
        receiveIndex.set(0);
        lastNettyMsg = null;
        lastNettyTime = 0;
    }

    @Override
    public void run() {
        try {
            // 订阅多个主题
            consumer.subscribe(Pattern.compile(topicPatten));

            log.info("开始监听Kafka主题: " + topicPatten);

            // 持续消费消息，直到被停止
            while (isRunning) {
                if (udpAsk) {
                    // 收到server应答后，需要commit
                    if (receiveIndex.get() > 0 ) {
                        if (this.consumer != null) {
                            log.info("commitAsync");
                            this.kafkaCommit();
                        }
                    }
                    // 是否要拉取新的消息
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
                                if (record.value() == null) {
                                    consumer.commitAsync();
                                    receiveIndex.set(0);
                                    lastNettyMsg = null;
                                    lastNettyTime = 0;
                                    continue;
                                }
                                log.info("收到消息: value = {}", record.value());
                                // 对kafka数据精简

                                ObjectMapper objectMapper = new ObjectMapper();
                                Map<String, Object> mapKafka = objectMapper.readValue(record.value(), Map.class);


                                Map payload = (Map<String, Object>)mapKafka.get("payload");

                                if (payload != null && payload.get("op").equals("r")) {
                                    this.kafkaCommit();
                                    continue;
                                }
                                if (payload == null) {
                                    payload = mapKafka;
                                }
                                // 提取事件基本信息
                                Map<String, Object> before = (Map<String, Object>) payload.get("before");
                                Map<String, Object> after = (Map<String, Object>) payload.get("after");
                                String op = (String) payload.get("op");
                                Map<String, Object> source = (Map<String, Object>) payload.get("source");
                                String table = null;
                                if (source != null) {
                                    table = (String) source.get("table");
                                }
                                Map<String, Object> mapNetty = new HashMap<>();
                                mapNetty.put("b", before);
                                mapNetty.put("a", after);
                                mapNetty.put("o", op);
                                mapNetty.put("t", table);
                                String jsonNetty = objectMapper.writeValueAsString(mapNetty);
                                // String jsonNetty = gson.toJson(mapNetty);
                                // 在这里可以添加自己的消息处理逻辑
                                processMessage(jsonNetty);
                                lastNettyMsg = jsonNetty;
                                lastNettyTime = System.currentTimeMillis();
                                produceIndex.set(1);

                            }
                        }
                    }else {
                        long currentTimeMillis = System.currentTimeMillis();
                        // 如果超时20s没有收到应答
                        if (lastNettyTime != 0 && (currentTimeMillis - lastNettyTime) > (long)20000) {
                            produceIndex.set(0);
                            receiveIndex.set(0);
                            lastNettyTime = currentTimeMillis;
                        }
                    }
                }else {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("收到消息1: value = {}", record.value());
                        ObjectMapper objectMapper = new ObjectMapper();
                        Map<String, Object> mapKafka = objectMapper.readValue(record.value(), Map.class);


                        Map payload = (Map<String, Object>)mapKafka.get("payload");

                        if (payload != null && payload.get("op").equals("r")) {
                            consumer.commitAsync();
                            continue;
                        }
                        if (payload == null) {
                            payload = mapKafka;
                        }
                        // 提取事件基本信息
                        Map<String, Object> before = (Map<String, Object>) payload.get("before");
                        Map<String, Object> after = (Map<String, Object>) payload.get("after");
                        String op = (String) payload.get("op");
                        Map<String, Object> source = (Map<String, Object>) payload.get("source");
                        String table = null;
                        if (source != null) {
                            table = (String) source.get("table");
                        }
                        Map<String, Object> mapNetty = new HashMap<>();
                        mapNetty.put("b", before);
                        mapNetty.put("a", after);
                        mapNetty.put("o", op);
                        mapNetty.put("t", table);
                        String jsonNetty = objectMapper.writeValueAsString(mapNetty);
                        // String jsonNetty = gson.toJson(mapNetty);
                        // 在这里可以添加自己的消息处理逻辑
                        processMessage(jsonNetty);
                        consumer.commitAsync();
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

    // 消息处理方法，可以根据实际需求重写
    private void processMessage(String value) {
        // 这里添加消息处理逻辑
//        String cypher = parseTransaction(value.strip());
//        log.info("生成的Cypher语句: {}", cypher);
//        if(cypher.strip().length() > 0) {
//            nettyClient.sendSyncMsg(cypher,this);
//        }
        nettyClient.sendSyncMsg(value,this);
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
}
