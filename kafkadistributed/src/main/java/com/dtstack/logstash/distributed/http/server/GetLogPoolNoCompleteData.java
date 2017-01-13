package com.dtstack.logstash.distributed.http.server;

import com.dtstack.logstash.distributed.ZkDistributed;
import com.dtstack.logstash.distributed.http.server.callback.ApiCallback;
import com.dtstack.logstash.distributed.http.server.callback.ApiCallbackMethod;
import com.dtstack.logstash.distributed.http.server.callback.ApiResult;
import com.dtstack.logstash.distributed.logmerge.LogPool;
import com.sun.javafx.tools.packager.Log;
import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by sishu.yss on 2017/1/13.
 */
public class GetLogPoolNoCompleteData extends PostHandler{

    private final static Logger logger = LoggerFactory
            .getLogger(ImmediatelyLoadNodeDataHandler.class);


    private ZkDistributed zkDistributed;

    public GetLogPoolNoCompleteData(ZkDistributed zkDistributed){
        this.zkDistributed = zkDistributed;
    }

    @Override
    public void handle(HttpExchange he) throws IOException {
        // TODO Auto-generated method stub

        ApiCallbackMethod.doCallback(new ApiCallback(){
            @Override
            public void execute(ApiResult apiResult) throws Exception {
                // TODO Auto-generated method stub
                apiResult.setData(LogPool.getInstance().getNotCompleteLog());
            }
        }, he);

    }
}
