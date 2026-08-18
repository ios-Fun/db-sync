package com.changgeng.netty;


import com.changgeng.kafka.KafkaConsumerThread;
import com.changgeng.proto.SqlInfoNeo4j;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.extern.slf4j.Slf4j;


/**
 * NettyHandler is the handler for {@link NettyClient}.
 * @author Sameer Narkhede See <a href="https://narkhedesam.com">https://narkhedesam.com</a>
 * @since Sept 2020
 *
 */
@Slf4j
public class NettyHandler extends ChannelInboundHandlerAdapter {

    public KafkaConsumerThread kafkaConsumerThread;

    private NettyClient nettyClient;

    public NettyHandler(NettyClient nettyClient) {
        this.nettyClient = nettyClient;
    }


    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        SqlInfoNeo4j.Sql sql = (SqlInfoNeo4j.Sql) msg;
        // ctx.close();
        this.kafkaConsumerThread.commitAsync(sql.getIsSuccess());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

}

