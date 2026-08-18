package com.changgeng.netty;


import com.changgeng.proto.SqlInfoNeo4j;
import com.google.gson.Gson;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import com.changgeng.service.ParseMsgService;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangyouzhuo
 */
@Slf4j
public class NettyHandler extends ChannelInboundHandlerAdapter {
    ParseMsgService parseMsgService;
    public NettyHandler(ParseMsgService parseMsgService) {
        this.parseMsgService = parseMsgService;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {        // DatagramPacket packet = (DatagramPacket) msg;
        SqlInfoNeo4j.Sql sql = (SqlInfoNeo4j.Sql) msg;
        boolean flag = parseMsgService.processCDCMessage(sql);
        SqlInfoNeo4j.Sql sqlInfo = SqlInfoNeo4j.Sql.newBuilder().setIsSuccess(flag).build();
        ctx.writeAndFlush(sqlInfo).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }
}

