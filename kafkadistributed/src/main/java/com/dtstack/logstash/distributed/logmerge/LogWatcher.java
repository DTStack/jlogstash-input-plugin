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
package com.dtstack.logstash.distributed.logmerge;
import com.dtstack.logstash.assembly.qlist.InputQueueList;
import com.dtstack.logstash.inputs.BaseInput;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * 监控数据,如果出现新事件则触发
 * Date: 2016/12/30
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */

public class LogWatcher implements Callable {

    private static final Logger logger = LoggerFactory.getLogger(LogWatcher.class);

    /**最大空闲等待2min*/
    private static int MAX_WAIT_PERIOD =  1 * 60;

    private static int DEAL_TIME_OUT_PERIOD = 1 * 60 * 1000;

    private long lastDealTimeout = System.currentTimeMillis();

    private boolean isRunning = false;

    private BlockingQueue<String> signal = new LinkedBlockingQueue<>();

    private ExecutorService executorService;

    private LogPool logPool;

    private static Gson gson = new Gson();

    private static InputQueueList inputQueueList = BaseInput.getInputQueueList();

    public  LogWatcher(LogPool logPool){
        this.logPool = logPool;
        if (inputQueueList == null){
            logger.error("not init InputQueueList. please check it.");
            System.exit(-1);
        }
    }

    public void wakeup(String flag){
        signal.offer(flag);
    }


    @Override
    public Object call() throws Exception {
        while(isRunning){
            String flag = signal.poll(MAX_WAIT_PERIOD, TimeUnit.SECONDS);
            if(flag == null){
                dealTimeout();
                continue;
            }

            CompletedLog completeLog = logPool.mergeLog(flag);
            if(completeLog == null){
                dealTimeout();
                continue;
            }

            if(logPool.hasNext(flag)){
                wakeup(flag);
            }

            logger.debug("add cms to inputqueulist:{}", gson.toJson(completeLog));
            inputQueueList.put(completeLog.getEventMap());
            dealTimeout();
        }
        logger.info("log pool watcher is not running....");
        return null;
    }

    private void dealTimeout(){
        if(lastDealTimeout + DEAL_TIME_OUT_PERIOD < System.currentTimeMillis()){
            logPool.dealTimeout();
        }
    }


    public void startup(){
        this.isRunning = true;
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(this);
        logger.info("log pool merge watcher is start up success");
    }

    public void shutdown(){
        this.isRunning = false;
        executorService.shutdown();
        logger.info("log pool merge watcher is shutdown");
    }
}
