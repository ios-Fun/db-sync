package com.changgeng.netty;


import com.changgeng.proto.SqlInfo;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
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
public class NettyHandler extends ChannelInboundHandlerAdapter {

    ParseMsgService parseMsgService;
    public NettyHandler(ParseMsgService parseMsgService) {
        this.parseMsgService = parseMsgService;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        // 读取客户端发送的数据 UserMOdel.User
        SqlInfo.Sql sql = (SqlInfo.Sql) msg;
        log.info("客户端发送的数据: {}, {}, {}, {}" + sql.getOp(),sql.getAfter(),sql.getBefore(),sql.getTable());
        boolean flag = parseMsgService.processCDCMessage(sql);
        SqlInfo.Sql sqlInfo = SqlInfo.Sql.newBuilder().setIsSuccess(flag).build();
        //向客户端发送消息
        ctx.writeAndFlush(sqlInfo).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

}

