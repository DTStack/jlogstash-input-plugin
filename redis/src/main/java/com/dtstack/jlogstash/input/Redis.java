package com.dtstack.jlogstash.input;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.inputs.BaseInput;
import com.dtstack.jlogstash.utils.DataType;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Date: 2017年6月15日
 * Company: www.dtstack.com
 *
 * @author jiangbo
 */

@SuppressWarnings("serial")
public class Redis extends BaseInput {

    @Required(required = true)
    private String host = "127.0.0.1";

    @Required(required = true)
    private String password;

    @Required(required = true)
    private int port = 6379;

    @Required(required = true)
    private String db;

    private static int timeout = 5000;

    @Required(required = true)
    private String key;

    @Required(required = true)
    private String data_type;

    @Required(required = true)
    private int data_size = 10;

    private boolean isStop = false;

    private String script_sha;

    private Jedis jedis;

    public Redis(Map config) {
        super(config);
    }

    public void initJedis() {
        jedis = new Jedis(host, port, timeout);
        jedis.auth(password);
    }

    public void prepare() {
        initJedis();
    }

    public void emit() {
        if (data_type.equals(DataType.LIST.getDataType())) {
            listRunner();
        } else if (data_type.equals(DataType.STRING.getDataType())) {
            stringRunner();
        } else if (data_type.equals(DataType.HASH.getDataType())) {
            // TODO hash operation
        } else {
            setRunner();
        }
    }

    public void release() {
        if (jedis != null) {
            isStop = true;
            jedis.close();
        }
    }

    /**
     *
     */
    private void setRunner(){
        while (!isStop) {
            Set<String> data = jedis.smembers(key);
            Iterator<String> iterator = data.iterator();
            Map<String,Object> map = new HashMap<String, Object>();
            while(iterator.hasNext()){
                map.put(key,iterator.next());
            }
            process(map);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private void stringRunner(){
        while (!isStop) {
            String data = jedis.get(key);
            Map<String,Object> map = new HashMap<String, Object>();
            map.put(key,data);
            process(map);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private void listRunner() {
        while (!isStop) {
            getList();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private void loadBatchScript() {
        String redisScript = "local batchsize = tonumber(ARGV[1]);\n"
                + "local result = redis.call('lrange', KEYS[1], 0, batchsize);\n"
                + "redis.call('ltrim', KEYS[1], batchsize + 1, -1);\n"
                + "return result;\n";

        script_sha = jedis.scriptLoad(redisScript);
    }

    /**
     *
     */
    private void getList() {
        loadBatchScript();
        List<String> keys = new ArrayList<String>();
        keys.add(key);

        List<String> args = new ArrayList<String>();
        args.add("" + data_size);

        List<String> data = (List<String>) jedis.evalsha(script_sha, keys, null);
        Map<String, Object> map = new HashMap<String, Object>();
        for (String item : data) {
            map.put(key, item);
        }
        process(map);
    }
}
