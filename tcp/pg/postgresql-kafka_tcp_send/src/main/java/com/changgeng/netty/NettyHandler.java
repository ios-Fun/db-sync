package com.changgeng.netty;


import com.changgeng.kafka.KafkaConsumerThread;
import com.changgeng.proto.SqlInfo;
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
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        log.error("TCPReadHandler exceptionCaught()");

        // 触发重连
        // nettyClient.resetConnect(false);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        SqlInfo.Sql sql = (SqlInfo.Sql) msg;
        // ctx.close();
        this.kafkaConsumerThread.commitAsync(sql.getIsSuccess());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

}

