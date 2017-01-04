package com.dtstack.logstash.kafkadistributed;

import com.dtstack.logstash.assembly.qlist.InputQueueList;
import com.dtstack.logstash.inputs.BaseInput;
import com.dtstack.logstash.logmerge.LogPool;
import com.dtstack.logstash.netty.server.NettyRev;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.util.Map;

/**
 * Reason:
 * Date: 2017/1/4
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */

public class MergeTest {

    private static Gson gson = new Gson();

    private static LogPool logPool;

    public static void init(){
        BaseInput.setInputQueueList(InputQueueList.getInputQueueListInstance(1, 1));
        logPool = LogPool.getInstance();
        NettyRev nettyRev = new NettyRev(8080);
        nettyRev.startup();
    }

    public void testMerge(){
    }

    public static void main(String[] args) {
        MergeTest.init();

        Map<String, Object> data = Maps.newHashMap();
        data.put("timestamp", System.currentTimeMillis() + "");
        data.put("host", "192.168.1.102");
        data.put("path", "/home/admin/data");
        data.put("logtype", "cmslog");
        //data.put("message", "2017-01-04T14:31:23.598+0800: 1810896.993: [GC (CMS Initial Mark) [1 CMS-initial-mark: 2781153K(3670016K)] 2834440K(4141888K), 0.0042808 secs] [Times: user=0.00 sys=0.00, real=0.01 secs]");
        data.put("message", "2016-12-28T10:08:15.480+0800: 1190308.875: [GC (Allocation Failure) 2016-12-28T10:08:15.480+0800: 1190308.875: [ParNew\n" +
                "Desired survivor size 26836992 bytes, new threshold 1 (max 6)\n" +
                "- age   1:   29590728 bytes,   29590728 total\n" +
                "- age   2:    1721480 bytes,   31312208 total");
        String jsonStr = gson.toJson(data);
        System.out.println(jsonStr);
        logPool.addLog(jsonStr);
    }
}
