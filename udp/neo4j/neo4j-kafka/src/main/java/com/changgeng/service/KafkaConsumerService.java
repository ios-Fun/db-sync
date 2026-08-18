package com.changgeng.service;

import com.changgeng.netty.NettyClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Order(value = 3)
public class KafkaConsumerService {

    @Autowired
    private NettyClient nettyClient;

    private Consumer consumer;

    // @KafkaListener(topics = {"creates", "updates", "deletes"}, groupId = "sync-group")
    public void listenToSyncTopics(String message, Consumer consumer) {
        log.info("message : {}", message);
        // nettyClient.sendSyncMsg(message);
        // String cypher = parseTransaction(message.strip());
        //System.out.println("生成的Cypher语句:");
        //System.out.println(cypher);
//        if(cypher.strip().length() > 0) {
//            //
//        }
//        nettyClient.sendSyncMsg(message, this.);
//        this.consumer = consumer;
    }

    public void kafkaCommit() {
        if (this.consumer != null) {
            //System.out.println("kafkaCommit:");
            this.consumer.commitAsync();
        }
    }


}
