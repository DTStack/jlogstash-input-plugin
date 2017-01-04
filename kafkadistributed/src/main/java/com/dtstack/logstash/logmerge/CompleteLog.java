package com.dtstack.logstash.logmerge;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * 一条完整的日志记录
 * Date: 2016/12/30
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */

public class CompleteLog {

    private String lineSP = System.getProperty("line.separator");

    private Map<String, Object> eventMap = Maps.newHashMap();

    private List<String> logInfo = Lists.newArrayList();

    public List<String> getLogInfo() {
        return logInfo;
    }

    public void setLogInfo(List<String> logInfo) {
        this.logInfo = logInfo;
    }

    public void addLog(String log){
        logInfo.add(log);
    }

    public Map<String, Object> getEventMap() {
        return eventMap;
    }

    public void setEventMap(Map<String, Object> eventMap) {
        this.eventMap = eventMap;
    }

    public void complete(){
        String msg = StringUtils.join(logInfo, lineSP);
        eventMap.put("message", msg);
    }

    @Override
    public String toString() {
        return logInfo.toString();
    }
}
