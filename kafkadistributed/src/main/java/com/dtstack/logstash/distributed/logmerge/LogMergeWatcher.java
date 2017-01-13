package com.dtstack.logstash.distributed.logmerge;

import com.dtstack.logstash.assembly.qlist.InputQueueList;
import com.dtstack.logstash.inputs.BaseInput;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * Reason:
 * Date: 2017/1/13
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */

public class LogMergeWatcher implements Callable{

    private static final Logger logger = LoggerFactory.getLogger(LogMergeWatcher.class);

    private boolean isRunning = false;

    private ExecutorService executorService;

    private LogPool logPool;

    /**merge 检查间隔时间*/
    private static int SLEEP_TIME = 5 * 1000;

    private static Gson gson = new Gson();

    private static InputQueueList inputQueueList = BaseInput.getInputQueueList();

    public LogMergeWatcher(LogPool logPool){
        this.logPool = logPool;
        if (inputQueueList == null){
            logger.error("not init InputQueueList. please check it.");
            System.exit(-1);
        }
    }

    @Override
    public Object call() throws Exception {
        while (isRunning){
            List<CompletedLog> rstLog = logPool.mergeLog();
            logger.warn("get full cmsMsg:{}", gson.toJson(rstLog));
            for(CompletedLog completedLog : rstLog){
                inputQueueList.put(completedLog.getEventMap());
            }

            Thread.sleep(SLEEP_TIME);
        }
        return null;
    }

    public void startup(){
        this.isRunning = true;
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(this);
        logger.info("log pool merge watcher is start up success, SLEEP_TIME:{}", SLEEP_TIME);
    }

    public void shutdown(){
        this.isRunning = false;
        executorService.shutdown();
        logger.info("log pool merge watcher is shutdown");
    }


}
