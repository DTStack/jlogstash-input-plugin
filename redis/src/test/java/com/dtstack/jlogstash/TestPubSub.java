package com.dtstack.jlogstash;

import redis.clients.jedis.JedisPubSub;

/**
 * Created by Administrator on 2017/6/16.
 */
public class TestPubSub extends JedisPubSub {

    @Override
    public void onMessage(String channel, String message) {
        System.out.println("channel:"+channel+","+"message:"+message);
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
        System.out.println("pattern:"+pattern+","+"channel:"+channel+","+"message:"+message);
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        System.out.println("channel:"+channel+","+"subscribedChannels:"+subscribedChannels);
    }


    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        System.out.println("channel:"+channel+","+"subscribedChannels:"+subscribedChannels);
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
        System.out.println("pattern:"+pattern+","+"subscribedChannels:"+subscribedChannels);
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
        System.out.println("pattern:"+pattern+","+"subscribedChannels:"+subscribedChannels);
    }
}
