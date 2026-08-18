package com.changgeng;

import com.changgeng.kafka.KafkaService;
import com.changgeng.util.SpringContextUtil;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication

@EnableAsync
public class PostgresqlKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(PostgresqlKafkaApplication.class, args);
		KafkaService kafkaService = SpringContextUtil.getBean(KafkaService.class);
		kafkaService.startKafka();
	}

}
