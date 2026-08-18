package com.changgeng.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class DataBaseConfig {

    @Value("${spring.datasource.druid.driver-class-name}")
    public String driver;

    @Value("${spring.datasource.druid.url}")
    public String url;

    @Value("${spring.datasource.druid.username}")
    public String username;

    @Value("${spring.datasource.druid.password}")
    public String password;

    @Value("${spring.datasource.druid.initial-size:1}")
    public String initialSize;

    @Value("${spring.datasource.druid.min-idle}")
    public String minIdle;

    @Value("${spring.datasource.druid.max-active}")
    public String maxActive;

    @Value("${spring.datasource.druid.max-wait}")
    public String maxWait;

    @Bean
    public DataSource dao() {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driver);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setInitialSize(Integer.parseInt(initialSize));
        dataSource.setMinIdle(Integer.parseInt(minIdle));
        dataSource.setMaxActive(Integer.parseInt(maxActive));
        dataSource.setMaxWait(Long.parseLong(maxWait));
        dataSource.setPoolPreparedStatements(true);
        dataSource.setValidationQuery("select version()");
        return  dataSource;
    }

}
