package com.changgeng.netty;


import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import com.changgeng.service.ParseMsgService;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.Charset;


/**
 * NettyHandler is the handler for {@link NettyServer}.
 * @author Sameer Narkhede See <a href="https://narkhedesam.com">https://narkhedesam.com</a>
 * @since Sept 2020
 *
 */
@Slf4j
public class NettyHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    ParseMsgService parseMsgService;
    public NettyHandler(ParseMsgService parseMsgService) {
        this.parseMsgService = parseMsgService;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
        DatagramPacket packet = (DatagramPacket) msg;
        String message = packet.content().toString(Charset.defaultCharset());
        log.info("Received Message : " + message);
        boolean flag = parseMsgService.processCDCMessage(message);
        // 暂时都是成功，否则一直无法消费kafka
        flag = true;
        byte byteFlag = flag ? (byte) 1:(byte)0;
        // byte byteFlag = (byte)0;
        byte[] bytes =  new byte[1];
        log.info("write response : " + byteFlag);
        bytes[0] = byteFlag;
        DatagramPacket data = new DatagramPacket(Unpooled.copiedBuffer(bytes), msg.sender());
        //向客户端发送消息
        ctx.writeAndFlush(data).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

}

