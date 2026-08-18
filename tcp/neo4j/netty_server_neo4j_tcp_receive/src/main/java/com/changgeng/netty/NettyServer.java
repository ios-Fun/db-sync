package com.changgeng.netty;

import com.changgeng.proto.SqlInfoNeo4j;
import com.changgeng.service.ParseMsgService;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
    @Value("${netty.server.port}")
    String port;

    private Channel channel;
    EventLoopGroup workerGroup;
    @Autowired
    ParseMsgService parseMsgService;

    @PostConstruct
    public void start() throws InterruptedException {
        log.info("netty server start");
        EventLoopGroup boss = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap server = new ServerBootstrap()
                .group(boss, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                        pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE,
                                0, 4, 0, 4));
                        pipeline.addLast("encoder", new ProtobufEncoder()); // protobuf 编码器
                        // 需要指定要对哪种对象进行解码
                        pipeline.addLast("decoder", new ProtobufDecoder(SqlInfoNeo4j.Sql.getDefaultInstance()));
                        pipeline.addLast(new NettyHandler(parseMsgService));
                    }
                });
        ChannelFuture f = server.bind(new InetSocketAddress(Integer.valueOf(port))).sync();
        log.info("TCP服务器已启动，监听端口: " + port);
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

