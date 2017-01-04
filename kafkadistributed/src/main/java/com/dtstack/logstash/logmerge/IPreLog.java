package com.dtstack.logstash.logmerge;

import java.util.List;
import java.util.Map;

/**
 * 未处理日志存放接口
 * Date: 2016/12/30
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */
public interface IPreLog {

    /**
     * 判断是不是一个完整的日志逻辑
     * @return
     */
    boolean checkIsCompleteLog();

    /**
     * 执行日志合并
     * @return
     */
    CompletedLog mergeGcLog();

    /**
     * 添加一条日志源
     * @param addLog
     * @return
     */
    boolean addLog(ClusterLog addLog);

    ClusterLog remove(int index);

    boolean remove(ClusterLog log );

    List<Map<String,Object>> getNotCompleteLog();

    /***
     * 暂时未想到比较好的解决办法,当前处理是每次超过过期时间就删除第一条数据
     */
    void dealTimeout();
}
