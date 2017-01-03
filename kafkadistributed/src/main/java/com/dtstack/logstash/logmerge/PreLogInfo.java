package com.dtstack.logstash.logmerge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * 接收发送过来的日志容器,日志根据时间排序(升序)
 * FIXME 当前仅针对一个用户的一个日志文件,需要改成用户&文件的-->list  数据结构
 * FIXME 需要判断每条记录的过期时间
 * Date: 2016/12/28
 * Company: www.dtstack.com
 * @ahthor xuchao
 */

public class PreLogInfo implements IPreLog {

    private static final Logger logger = LoggerFactory.getLogger(PreLogInfo.class);

    /**该日志的标识:用户加上文件路径*/
    private String flag = "";

    private long firstEleTime;

    private CMSLogMerge logMerge = new CMSLogMerge();

    private List<ClusterLog> logList;

    public PreLogInfo(String flag){
        this.flag  = flag;
        logList = new LinkedList<>();
    }

    /**
     * 需要根据规则判断是否可以加入到该队列中
     * @param addLog
     * @return
     */
    public boolean addLog(ClusterLog addLog){//插入的时候根据时间排序,升序
        int addPos = logList.size();
        for(int i=0; i<logList.size(); i++){
            ClusterLog compLog = logList.get(i);
            if(addLog.getLogTime() < compLog.getLogTime()){
                addPos = i;
                break;
            }
        }

        logList.add(addPos, addLog);
        if(logList.size() == 1){
            firstEleTime = System.currentTimeMillis();
        }

        if (logList.size() >= CMSLogMerge.MERGE_NUM){
            LogPool.getInstance().addMergeSignal(flag);
        }
        return true;
    }

    /**
     * 合并出完整的一条日志
     * @return
     */
    @Override
    public GCLog mergeGcLog(){

        if(!checkIsCompleteLog()){
           return null;
        }

        //从列表中抽取出CMS记录
        GCLog cmsLog = new GCLog();
        for (int i=0; i<CMSLogMerge.MERGE_NUM; i++){
            ClusterLog currLog = logList.remove(0);//一直remove第0个
            if(currLog == null){
               break;
            }

            cmsLog.addLog(currLog.getLoginfo());
        }
        return cmsLog;
    }

    public ClusterLog remove(int index){
        return logList.remove(index);
    }

    public boolean remove(ClusterLog log ){
        return logList.remove(log);
    }

    /**
     * 判断是不是一条完整的CMS日志
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
    public String toString() {
        return logList.toString();
    }
}
