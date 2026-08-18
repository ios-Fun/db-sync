package com.changgeng.service;

import com.changgeng.netty.NettyClient;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Order(value = 2)
public class NettyClientService {
    @Autowired
    private NettyClient nettyClient;

    @PostConstruct
    public void startNettyServer() {
        try {
            nettyClient.start();
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
    }
}
