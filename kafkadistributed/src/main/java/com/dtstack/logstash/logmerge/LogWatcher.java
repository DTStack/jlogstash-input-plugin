package com.dtstack.logstash.logmerge;

import java.util.concurrent.Callable;

/**
 * 监控数据,如果出现新事件则触发
 * Date: 2016/12/30
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */

public class LogWatcher implements Callable {

    @Override
    public Object call() throws Exception {
        //FIXME 检查每个文件的日志队列是否超时超时
        return null;
    }
}
