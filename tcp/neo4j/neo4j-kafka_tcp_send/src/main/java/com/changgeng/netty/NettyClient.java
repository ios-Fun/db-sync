package com.changgeng.netty;

import com.changgeng.kafka.KafkaConsumerThread;
import com.changgeng.proto.SqlInfoNeo4j;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
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

    NettyHandler nettyHandler = new NettyHandler(this);

    @Async
    public void start() throws InterruptedException {
        log.info("netty start");
        try{
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workGroup)
                    .channel(NioSocketChannel.class) // 指定客户端通道类型
                    .option(ChannelOption.SO_KEEPALIVE, true) // 开启心跳
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 2. 编解码器配置（与服务端一致，顺序不可乱）
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                            pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE,
                                    0, 4, 0, 4));
                            pipeline.addLast("encoder", new ProtobufEncoder()); // protobuf 编码器
                            pipeline.addLast("decoder", new ProtobufDecoder(SqlInfoNeo4j.Sql.getDefaultInstance()));
                            pipeline.addLast(nettyHandler); // 自定义客户端处理器
                        }
                    });
            ChannelFuture channelFuture = bootstrap.connect(remoteHost, Integer.valueOf(port));

            channel = channelFuture.channel();
            // 等待通道关闭
            channel.closeFuture().await();
        }finally{
            workGroup.shutdownGracefully();
        }
    }

    // netty发送udp消息
    public void sendSyncMsg(SqlInfoNeo4j.Sql sql, KafkaConsumerThread kafkaConsumerThread) {
        if (channel == null) {
            log.warn("channel is null");
            return;
        }
        // netty收到应答后，需要提交kafka的commit
        nettyHandler.kafkaConsumerThread = kafkaConsumerThread;
        // 发送数据包
        channel.writeAndFlush(sql);
        log.info("已发送消息: " + sql);
    }

    /**
     *	Shutdown a client
     */
    public void shutdown(){
        workGroup.shutdownGracefully();
    }
}

