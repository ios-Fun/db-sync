package com.changgeng;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class PostgresqlKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(PostgresqlKafkaApplication.class, args);
	}

}
