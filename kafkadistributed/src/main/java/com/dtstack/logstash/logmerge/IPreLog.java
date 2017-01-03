package com.dtstack.logstash.logmerge;

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
    GCLog mergeGcLog();

    /**
     * 添加一条日志源
     * @param addLog
     * @return
     */
    boolean addLog(ClusterLog addLog);

    ClusterLog remove(int index);

    boolean remove(ClusterLog log );
}
