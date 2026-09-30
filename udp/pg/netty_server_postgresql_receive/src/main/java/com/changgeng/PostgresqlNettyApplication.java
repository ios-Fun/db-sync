package com.changgeng;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class PostgresqlNettyApplication {

	public static void main(String[] args) {
		SpringApplication.run(PostgresqlNettyApplication.class, args);
	}

}
