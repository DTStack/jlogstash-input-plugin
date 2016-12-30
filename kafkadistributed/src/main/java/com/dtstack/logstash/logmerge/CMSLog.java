package com.dtstack.logstash.logmerge;

import com.google.common.collect.Lists;
import java.util.List;

/**
 * 一条CMSGC日志信息
 * Date: 2016/12/30
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */

public class CMSLog {

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

    @Override
    public String toString() {
        return logInfo.toString();
    }
}
