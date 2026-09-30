package com.changgeng.kafka;

import com.changgeng.writer.WalFileWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class WalKafkaConsumerService {

    @Autowired
    private WalFileWriter walFileWriter;

    /**
     * 高并发消费配置说明：
     * 1. concurrency 建议设置为 Kafka Topic 的分区数（如6），实现真正并行消费
     * 2. 通过 @Header 获取分区号，确保同一分区的数据写入同一个文件，保证局部有序
     * 3. ack-mode 建议使用 MANUAL_IMMEDIATE 或 RECORD，配合业务做精确提交
     */
    @KafkaListener(
        topics = "${kafka.topic.wal}",
        groupId = "${kafka.consumer.group-id}",
        concurrency = "${kafka.consumer.concurrency:6}"
    )
    public void consume(
            String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition
    ) {
        try {
            walFileWriter.write(message, partition);
        } catch (Exception e) {
            log.error("WAL写入失败, partition={}, message={}", partition, message, e);
            throw new RuntimeException("WAL写入失败", e);
        }
    }
}
