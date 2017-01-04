package com.dtstack.logstash.logmerge;

import com.dtstack.logstash.assembly.qlist.InputQueueList;
import com.dtstack.logstash.gclog.CMSPreLogInfo;
import com.dtstack.logstash.inputs.BaseInput;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 * FIXME 暂时未考虑多线程
 * FIXME 考虑太久未使用的内存的清理
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

    public void addLog(String log){

        ClusterLog clusterLog = ClusterLog.generateClusterLog(log);
        if(log == null){
            logger.info("analyse msg from log err:{}.", log);
            return;
        }

        String flag = clusterLog.getLogFlag();
        IPreLog preLogInfo = logInfoMap.get(flag);
        if(preLogInfo == null){
            preLogInfo = createPreLogInfoByLogType(clusterLog);
            if (preLogInfo == null) return;
            logInfoMap.put(flag, preLogInfo);
        }

        preLogInfo.addLog(clusterLog);
    }

    public CompleteLog mergeLog(String flag){
        IPreLog preLogInfo = logInfoMap.get(flag);
        return preLogInfo.mergeGcLog();
    }

    public void addMergeSignal(String flag){
        logWatcher.wakeup(flag);
    }

    private IPreLog createPreLogInfoByLogType(ClusterLog clusterLog){
        if(LogTypeConstant.CMS_LOG_TYPE.equalsIgnoreCase(clusterLog.getLogFlag())){
            return new CMSPreLogInfo(clusterLog.getLogFlag());
        }else{
            logger.info("not support log type of {}.", clusterLog.getLogType());
            logger.info("original log is {}.", clusterLog.getOriginalLog());
            return null;
        }
    }

    //FIXME 获取未完成的日志信息
    public List<Map<String,Object>> getNotCompleteLog(){
        return  null;
    }
}
