package com.dtstack.logstash.inputs;

import io.netty.channel.ChannelHandlerContext;


public interface IMessageListener {
    public void onNewMessage(ChannelHandlerContext ctx, Message message);
    public void onNewConnection(ChannelHandlerContext ctx);
    public void onConnectionClose(ChannelHandlerContext ctx);
    public void onException(ChannelHandlerContext ctx,Throwable cause);
}