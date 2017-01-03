package com.dtstack.logstash.logmerge;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 日志信息,包含日志发送时间
 * Date: 2016/12/28
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */

public class ClusterLog {

    private static final Logger logger = LoggerFactory.getLogger(ClusterLog.class);

    private static Gson gson = new Gson();

    private long logTime;

    private String host;

    private String path;

    private String loginfo;

    private String originalLog;

    public long getLogTime() {
        return logTime;
    }

    public void setLogTime(long logTime) {
        this.logTime = logTime;
    }

    public String getLoginfo() {
        return loginfo;
    }

    public void setLoginfo(String loginfo) {
        this.loginfo = loginfo;
    }

    @Override
    public String toString() {
        return loginfo;
    }

    public String getLogFlag(){
        return host + ":" + path;
    }

    public static ClusterLog generateClusterLog(String log) {
        Map<String, String> eventMap = null;
        try{
           eventMap = gson.fromJson(log, Map.class);
        }catch (Exception e){
            logger.error("解析 gc json 对象异常", e);
            return null;
        }

        ClusterLog clusterLog = new ClusterLog();
        long time = Long.valueOf(eventMap.get("timestamp"));
        String msg = eventMap.get("message");
        clusterLog.setLogTime(time);
        clusterLog.setLoginfo(msg);
        clusterLog.originalLog = log;

        return clusterLog;
    }
}
