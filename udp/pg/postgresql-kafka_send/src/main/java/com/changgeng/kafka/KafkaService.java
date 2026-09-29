package com.changgeng.kafka;

import com.changgeng.netty.NettyClient;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.Map;

/**
 * @author wangyouzhuo
 */
@Service
@Order(value = 3)
public class KafkaService {

    KafkaConsumerThread kafkaConsumerThread;

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrapServers;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    String autoOffsetReset;

    @Value("$(udp.ask)")
    public Boolean udpAsk;

    @Autowired
    private NettyClient nettyClient;


    // @PostConstruct
    public void startKafka() {

        String groupId = "sync-group";
        String topicPatten = "sync.*";
        kafkaConsumerThread = new KafkaConsumerThread(bootstrapServers, groupId, topicPatten, autoOffsetReset,udpAsk, nettyClient);
        Thread thread = new Thread(kafkaConsumerThread);
        thread.start();
    }

//    public void kafkaCommit() {
//        if (this.kafkaConsumerThread != null) {
//            System.out.println("kafkaCommit:");
//            this.kafkaConsumerThread.commitAsync();
//        }
//    }


}
