package com.dtstack.jlogstash.input;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.inputs.BaseInput;
import redis.clients.jedis.Jedis;
import java.util.Map;

/**
 * Date: 2017年6月15日
 * Company: www.dtstack.com
 * @author jiangbo
 */

@SuppressWarnings("serial")
public class Redis extends BaseInput {

    @Required(required = true)
    private String host = "127.0.0.1";

    private String password;

    private int port = 6379;

    private static int timeout = 5000;

    @Required(required = true)
    private String key;

    @Required(required = true)
    private String data_type;

    private Jedis jedis;

    public Redis(Map config){
        super(config);
    }

    public void initJedis(){
        jedis = new Jedis(host,port,timeout);
        jedis.auth(password);
    }

    public void prepare() {
        initJedis();
    }

    public void emit() {

    }

    public void release() {
        if(jedis != null){
            jedis.close();
        }
    }

}
