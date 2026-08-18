package com.changgeng.config;


import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class BaseDao {

    @Value("${spring.datasource.driverClassName}")
    public String driver;

    @Value("${spring.datasource.url}")
    public String url;

    @Value("${spring.datasource.username}")
    public String username;

    @Value("${spring.datasource.password}")
    public String password;

    @Value("${spring.datasource.initiaSize}")
    public String initialSize;

    @Value("${spring.datasource.minIdle}")
    public String minIdle;

    @Value("${spring.datasource.maxActive}")
    public String maxActive;

    @Value("${spring.datasource.maxWait}")
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
        dataSource.setValidationQuery("match (n) return id(n) limit 2");
        return  dataSource;
    }
}

