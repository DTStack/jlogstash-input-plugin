package com.dtstack.logstash.distributed.logmerge;

import com.dtstack.logstash.distributed.util.RouteUtil;
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

    private String logType;

    private String flag;

    public String getLogFlag(){
       return this.flag;
    }

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

    public String getOriginalLog() {
        return originalLog;
    }

    public void setOriginalLog(String originalLog) {
        this.originalLog = originalLog;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    /**
     * 获取除了message字段以外的信息
     * @return
     */
    public Map<String, Object> getBaseInfo(){
        Map<String, Object> eventMap = null;
        try{
            eventMap = gson.fromJson(originalLog, Map.class);
        }catch (Exception e){
            logger.error("解析 log json 对象异常", e);
            return null;
        }

        eventMap.remove("message");
        return eventMap;
    }

    /**
     * 获取除了message字段以外的信息
     * @return
     */
    public Map<String, Object> getEventMap(){
        Map<String, Object> eventMap = null;
        try{
            eventMap = gson.fromJson(originalLog, Map.class);
        }catch (Exception e){
            logger.error("解析 log json 对象异常", e);
            return null;
        }

        return eventMap;
    }

    public static ClusterLog generateClusterLog(String log) {
        Map<String, Object> eventMap = null;
        try{
           eventMap = gson.fromJson(log, Map.class);
        }catch (Exception e){
            logger.error("解析 log json 对象异常", e);
            return null;
        }

        ClusterLog clusterLog = new ClusterLog();
        long time = getMillTimestamp(eventMap);
        String msg = (String)eventMap.get("message");
        String host = (String)eventMap.get("host");
        String path = (String)eventMap.get("path");
        String logType = (String)eventMap.get("logtype");
        clusterLog.setLogTime(time);
        clusterLog.setLoginfo(msg);
        clusterLog.originalLog = log;
        clusterLog.host = host;
        clusterLog.path = path;
        clusterLog.logType = logType;
        clusterLog.flag = RouteUtil.getFormatHashKey(eventMap);
        return clusterLog;
    }

    private static Long getMillTimestamp(Map<String,Object> event){
        return 0l;
    }
}
