package com.dtstack.logstash.logmerge;

import java.util.List;

/**
 * 日志合并接口
 * Date: 2016/12/30
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */
public interface IlogMerge {

    /**
     * 判断是不是一个完整的日志逻辑
     * @param logPool
     * @return
     */
    boolean checkisCompleteLog(List<ClusterLog> logPool);

}
