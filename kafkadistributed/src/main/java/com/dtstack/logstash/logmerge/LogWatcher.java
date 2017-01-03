package com.dtstack.logstash.logmerge;

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

    public  LogWatcher(LogPool logPool){
        this.logPool = logPool;
    }

    public void wakeup(String flag){
        signal.offer(flag);
    }


    @Override
    public Object call() throws Exception {
        while(isRunning){
            String flag = signal.poll(MAX_WAIT_PERIOD, TimeUnit.SECONDS);
            logPool.mergeLog(flag);
            //FIXME 将数据加入到jlogstash 的input数据流当中
        }

        return null;
    }

    public void startup(){
        this.isRunning = true;
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(this);
        logger.info("gc log pool merge watch is start up success");
    }

    public void shutdown(){
        this.isRunning = false;
        executorService.shutdown();
    }
}
