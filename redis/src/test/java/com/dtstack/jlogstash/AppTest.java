package com.dtstack.jlogstash;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {

    public AppTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(AppTest.class);
    }


    public void testApp() {
        //pubsub();
        listBatch();
        //listSingleListener("key3");
    }

    public void pubsub() {
        Jedis jedis = new Jedis("localhost", 6379, 100000);
        TestPubSub testPubSub = new TestPubSub();
        testPubSub.proceed(jedis.getClient(), "redisChat");
    }

    private void listSingleListener(String key) {
        Jedis jedis = getJedis();
        String redisScript = "return redis.call('lrange', KEYS[1],0,0)";

        List<String> keys = new ArrayList<String>();
        keys.add(key);

        List<String> args = new ArrayList<String>();
        args.add("0");

        List<String> response = (List<String>) jedis.eval(redisScript,keys,null);
        System.out.println(response.toString());
    }

    public void listBatch() {
        Jedis jedis = getJedis();

        String redisScript = "local result = redis.call('lrange', KEYS[1], 0, " + 2 + ");\n"
                + "redis.call('ltrim', KEYS[1], " + 2 + " + 1, -1);\n"
                + "return result;\n";

        List<String> keys = new ArrayList<String>();
        keys.add("key1");

        List<String> args = new ArrayList<String>();
        args.add("1");

        List<String> response = (List<String>) jedis.eval(redisScript, keys, args);

        System.out.print(response.toString());
    }


    public Jedis getJedis() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(true);
        JedisPool jedisPool = new JedisPool("localhost");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }
}
