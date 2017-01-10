/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.logstash.distributed.logmerge;

import com.dtstack.logstash.distributed.util.RouteUtil;
import com.dtstack.logstash.exception.ExceptionUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
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

    private static ObjectMapper objectMapper = new ObjectMapper();

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
            eventMap = objectMapper.readValue(originalLog,Map.class);
            eventMap.remove("message");
        }catch (Exception e){
            logger.error("parse log json error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return eventMap;
    }

    /**
     * 获取所有字段信息
     * @return
     */
    public Map<String, Object> getEventMap(){
        Map<String, Object> eventMap = null;
        try{
            eventMap = objectMapper.readValue(originalLog,Map.class);
        }catch (Exception e){
            logger.error("parse log json error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return eventMap;
    }

    public static ClusterLog generateClusterLog(String log) {
        Map<String, Object> eventMap = null;
        try{
            eventMap = objectMapper.readValue(log,Map.class);
        }catch (Exception e){
            logger.error("parse log json error:{}", ExceptionUtil.getErrorMessage(e));
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
        String timestamp = (String)event.get("@timestamp");
        return DateTime.parse(timestamp).getMillis();
    }
}
