package com.changgeng.netty;

import com.changgeng.service.ParseMsgService;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;


/**
 * Client is the Netty UDP client.
 * @author Sameer Narkhede See <a href="https://narkhedesam.com">https://narkhedesam.com</a>
 * @since Sept 2020
 *
 */
@Slf4j
@Component
public class NettyServer {
    String remoteHost = "192.168.0.58";
    int port = 8888;

    private Channel channel;
    EventLoopGroup workerGroup;
    @Autowired
    ParseMsgService parseMsgService;

    @PostConstruct
    public void start() throws InterruptedException {
        log.info("netty server start");
        workerGroup = new NioEventLoopGroup();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    public void initChannel(NioDatagramChannel ch) {
                        // 设置自定义的处理器
                        ch.pipeline().addLast(new NettyHandler(parseMsgService));
                    }
        });
        ChannelFuture f = bootstrap.bind(new InetSocketAddress(port)).sync();
        log.info("UDP服务器已启动，监听端口: " + port);
        channel = f.channel();

        f.channel().closeFuture().sync();
    }

    /**
     *	Shutdown a client
     */
    public void shutdown(){

        this.workerGroup.shutdownGracefully();
    }
}

