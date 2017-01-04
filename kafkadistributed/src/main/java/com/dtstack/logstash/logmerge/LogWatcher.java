package com.dtstack.logstash.logmerge;

import com.dtstack.logstash.assembly.qlist.InputQueueList;
import com.dtstack.logstash.inputs.BaseInput;
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
    private static int MAX_WAIT_PERIOD =  2 * 60;

    private boolean isRunning = false;

    private BlockingQueue<String> signal = new LinkedBlockingQueue<>();

    private ExecutorService executorService;

    private LogPool logPool;

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
                continue; //FIXME 处理超时队列消息
            }

            CompleteLog completeLog = logPool.mergeLog(flag);
            inputQueueList.put(completeLog.getEventMap());
        }
        logger.info("log pool watcher is not running....");
        return null;
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
