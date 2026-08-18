package com.changgeng.netty;

import com.changgeng.kafka.KafkaConsumerThread;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.CharsetUtil;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
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
@Order(value = 1)
public class NettyClient{

    @Value("${netty.server.url}")
    String remoteHost;

    @Value("${netty.server.port}")
    String port;
    Channel channel;
    EventLoopGroup workGroup = new NioEventLoopGroup();

//    @Autowired
    // KafkaService kafkaService;

    NettyHandler nettyHandler = new NettyHandler();

    @Async
    public void start() throws InterruptedException {
        log.info("netty start");
        try{
            Bootstrap b = new Bootstrap();
            b.group(workGroup);
            b.channel(NioDatagramChannel.class);
            b.handler(new ChannelInitializer<DatagramChannel>() {
                protected void initChannel(DatagramChannel datagramChannel) throws Exception {
                    datagramChannel.pipeline().addLast(nettyHandler);
                }
            });
            ChannelFuture channelFuture = b.connect(remoteHost, Integer.valueOf(port));

            channel = channelFuture.channel();
            // 等待通道关闭
            channel.closeFuture().await();
        }finally{
        }
    }

    // netty发送udp消息
    public void sendSyncMsg(String  message, KafkaConsumerThread kafkaConsumerThread) {
        if (channel == null) {
            log.warn("channel is null");
            return;
        }
        // netty收到应答后，需要提交kafka的commit
        nettyHandler.kafkaConsumerThread = kafkaConsumerThread;
        // 创建要发送的数据包
        DatagramPacket packet = new DatagramPacket(
                Unpooled.copiedBuffer(message, CharsetUtil.UTF_8),
                new InetSocketAddress(remoteHost, Integer.valueOf(port))
        );

        // 发送数据包
        channel.writeAndFlush(packet);
        log.info("已发送消息: " + message);
    }

    /**
     *	Shutdown a client
     */
    public void shutdown(){
        workGroup.shutdownGracefully();
    }
}

