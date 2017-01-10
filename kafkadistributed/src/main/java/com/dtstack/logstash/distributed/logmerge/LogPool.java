package com.dtstack.logstash.distributed.logmerge;

import com.dtstack.logstash.distributed.gclog.CMSPreLogInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * FIXME 考虑超时未使用的内存的清理
 * Date: 2016/12/30
 * Company: www.dtstack.com
 * @ahthor xuchao
 */

public class LogPool {

    private static Logger logger = LoggerFactory.getLogger(LogPool.class);

    private Map<String, IPreLog> logInfoMap = Maps.newHashMap();

    private LogWatcher logWatcher;

    private static LogPool singleton = new LogPool();

    public static LogPool getInstance(){
        return singleton;
    }

    private LogPool(){
        init();
    }

    public void init(){
        logWatcher = new LogWatcher(this);
        logWatcher.startup();
    }

    public void addLog(String log) throws Exception{
        if(log == null){
            logger.info("analyse msg from log err:{}.", log);
            return;
        }
        ClusterLog clusterLog = ClusterLog.generateClusterLog(log);
        String flag = clusterLog.getLogFlag();
        IPreLog preLogInfo = logInfoMap.get(flag);
        if(preLogInfo == null){
            preLogInfo = createPreLogInfoByLogType(clusterLog);
            if (preLogInfo == null) return;
            logInfoMap.put(flag, preLogInfo);
        }
        preLogInfo.addLog(clusterLog);
    }

    public CompletedLog mergeLog(String flag){
        IPreLog preLogInfo = logInfoMap.get(flag);
        return preLogInfo.mergeGcLog();
    }

    public void dealTimeout(){
        for(IPreLog preLog : logInfoMap.values()){
            preLog.dealTimeout();
        }
    }

    public void addMergeSignal(String flag){
        logWatcher.wakeup(flag);
    }

    private IPreLog createPreLogInfoByLogType(ClusterLog clusterLog){
        if(LogTypeConstant.CMS_LOG_TYPE.equalsIgnoreCase(clusterLog.getLogType())){
            return new CMSPreLogInfo(clusterLog.getLogFlag());
        }else{
            logger.info("not support log type of {}.", clusterLog.getLogType());
            logger.info("original log is {}.", clusterLog.getOriginalLog());
            return null;
        }
    }

    /**
     * 获取未完成的日志信息
     */
    public List<Map<String,Object>> getNotCompleteLog(){
        List<Map<String, Object>> notCompleteList = Lists.newArrayList();
        for (IPreLog preLog : logInfoMap.values()){
            notCompleteList.addAll(preLog.getNotCompleteLog());
        }
        return  notCompleteList;
    }


    /**
     * 获取指定未完成的日志信息
     */
    public List<Map<String,Object>> getNotCompleteLog(List<String> nodes){
        List<Map<String, Object>> notCompleteList = Lists.newArrayList();
        Set<Map.Entry<String,IPreLog>> sets = logInfoMap.entrySet();
        for(Map.Entry<String,IPreLog> entry:sets){
          if(nodes.contains(entry.getKey())){
              notCompleteList.addAll(entry.getValue().getNotCompleteLog());
          }
        }
        return  notCompleteList;
    }
}
