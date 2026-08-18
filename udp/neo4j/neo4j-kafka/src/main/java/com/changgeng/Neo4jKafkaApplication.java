package com.changgeng;

import com.changgeng.kafka.KafkaService;
import com.changgeng.util.SpringContextUtil;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

//@SpringBootApplication(exclude = {
//		DataSourceAutoConfiguration.class,
//		DataSourceTransactionManagerAutoConfiguration.class,
//		HibernateJpaAutoConfiguration.class})

@EnableAsync
@SpringBootApplication
public class Neo4jKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(Neo4jKafkaApplication.class, args);
//		NettyClientService nettyClientService = SpringContextUtil.getBean(NettyClientService.class);
//		nettyClientService.startNettyServer();
		KafkaService kafkaService = SpringContextUtil.getBean(KafkaService.class);
		kafkaService.startKafka();
	}

}
