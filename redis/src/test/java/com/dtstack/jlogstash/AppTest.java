package com.dtstack.jlogstash;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Iterator;
import java.util.Set;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(true);
        JedisPool jedisPool = new JedisPool("localhost");
        Jedis jedis = jedisPool.getResource();
        Set<String> keys = jedis.keys("*");
        Iterator<String> iterator = keys.iterator();
        while(iterator.hasNext()){
            String key = iterator.next();
            System.out.println(key+":"+jedis.get(key));
        }
    }
}
