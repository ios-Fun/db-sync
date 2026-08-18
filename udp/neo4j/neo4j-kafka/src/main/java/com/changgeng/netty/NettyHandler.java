package com.changgeng.netty;


import com.changgeng.kafka.KafkaConsumerThread;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
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
public class NettyHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    public KafkaConsumerThread kafkaConsumerThread;

//    public NettyHandler(KafkaConsumerService kafkaConsumerService) {
//        this.kafkaConsumerService = kafkaConsumerService;
//    }


    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
        final ByteBuf buf = msg.content();
        int readableBytes = buf.readableBytes();
        byte[] content = new byte[readableBytes];
        buf.readBytes(content);
        log.info("Client received " + content[0] + " value in response");
        // ctx.close();
        this.kafkaConsumerThread.commitAsync(content[0] == (byte) 1);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

}

