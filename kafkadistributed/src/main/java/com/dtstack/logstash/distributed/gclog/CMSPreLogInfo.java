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
package com.dtstack.logstash.distributed.gclog;

import com.dtstack.logstash.distributed.logmerge.*;
import com.dtstack.logstash.inputs.BaseInput;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 接收发送过来的日志容器,日志根据时间排序(升序)
 * 当前仅针对一个用户的一个日志文件
 * FIXME 需要判断每条记录的过期时间
 * Date: 2016/12/28
 * Company: www.dtstack.com
 * @ahthor xuchao
 */

public class CMSPreLogInfo implements IPreLog {

    private static final Logger logger = LoggerFactory.getLogger(CMSPreLogInfo.class);

    /**整合一条cms消息的超时时间*/
    private static final int TIME_OUT = 60 * 1000;

    private long firstEleTime = 0;

    private CMSLogPattern logMerge = new CMSLogPattern();

    private List<ClusterLog> logList;

    private String flag;

    private static final Gson gson = new Gson();

    public CMSPreLogInfo(String flag){
        this.flag  = flag;
        logList = new CopyOnWriteArrayList<ClusterLog>();
    }

    /**
     * 需要根据规则判断是否可以加入到该队列中
     * @param addLog
     * @return
     */
    public boolean addLog(ClusterLog addLog){//插入的时候根据时间排序,升序

        if(!logMerge.checkIsFullGC(addLog.getLoginfo())){//非full gc 直接添加到inputlist
            Map<String, Object> eventMap = addLog.getOriginalLog();
            if(logMerge.checkIsYoungGC(addLog.getLoginfo())){//判断是不是younggc
                eventMap.put("gctype", GCTypeConstant.YOUNG_LOG_TYPE);
            }

            BaseInput.getInputQueueList().put(eventMap);
            return true;
        }

        logger.debug("offset:{},---cmslog:{}.", addLog.getOffset(), addLog.getLoginfo());
        int addPos = logList.size();
        for(int i=0; i<logList.size(); i++){
            ClusterLog compLog = logList.get(i);
            if(addLog.getOffset() < compLog.getOffset()){
                addPos = i;
                break;
            }
        }

        logList.add(addPos, addLog);
        if(logList.size() == 1){
            firstEleTime = System.currentTimeMillis();
        }

        if (logList.size() >= CMSLogPattern.MERGE_NUM){
            logger.debug("fullgc:{}.", gson.toJson(logList));
            LogPool.getInstance().addMergeSignal(flag);
        }
        return true;
    }


    /**
     * 合并出完整的一条日志
     * @return
     */
    @Override
    public CompletedLog mergeGcLog(){

        if(!checkIsCompleteLog()){
            return null;
        }

        //从列表中抽取出CMS记录
        CompletedLog cmsLog = new CompletedLog();
        for (int i = 0; i< CMSLogPattern.MERGE_NUM; i++){
            ClusterLog currLog = logList.remove(0);//一直remove第0个
            if(currLog == null){
                break;
            }

            if(i == 0){
                cmsLog.setEventMap(currLog.getBaseInfo());
            }

            cmsLog.addLog(currLog.getLoginfo());
        }

        Map<String, Object> extInfo = Maps.newHashMap();
        extInfo.put("gctype", GCTypeConstant.CMS_LOG_TYPE);
        cmsLog.complete(extInfo);
        if(logList.size() > 0){
            firstEleTime = System.currentTimeMillis();
        }

        return cmsLog;
    }

    @Override
    public ClusterLog remove(int index){
        return logList.remove(index);
    }

    @Override
    public boolean remove(ClusterLog log ){
        return logList.remove(log);
    }

    @Override
    public List<Map<String, Object>> getNotCompleteLog() {
        Gson gson = new Gson();
        List<Map<String, Object>> rstList = Lists.newArrayList();
        for (ClusterLog log : logList){
            rstList.add(log.getOriginalLog());
        }

        logger.warn("----getnotcomp----:{}", gson.toJson(rstList));

        return rstList;
    }

    /**
     * 判断是不是一条完整的日志
     * @return
     */
    public boolean checkIsCompleteLog(){
        boolean isCompleteLog = logMerge.checkIsCompleteLog(logList.subList(0, 12));
        if (isCompleteLog){
            logger.debug("get a full msg..");
        }

        return isCompleteLog;
    }

    @Override
    public void dealTimeout(){
        if(firstEleTime == 0){
            return;
        }

        if(logList.size() == 0){
            return;
        }

        boolean isTimeout = firstEleTime + TIME_OUT  < System.currentTimeMillis();
        if(isTimeout){//每次删除第一条记录
            ClusterLog log = logList.remove(0);
            firstEleTime = System.currentTimeMillis();
            logger.info("time out for cms log, delete log:{}", log.getOriginalLog());
        }
    }


    @Override
    public String toString() {
        return logList.toString();
    }
}
