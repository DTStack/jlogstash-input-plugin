package com.dtstack.jlogstash.inputs;

import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.dtstack.jlogstash.annotation.Required;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;


/**
 *
 * Reason: TODO ADD REASON(可选)
 * Date: 2018年8月28日
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 *
 */
public class Binlog extends BaseInput {

    private static final Logger logger = LoggerFactory.getLogger(Binlog.class);

    @Required(required = true)
    private String host;

    private int port = 3306;

    private long slaveId = 3344L;

    @Required(required = true)
    private String username;

    @Required(required = true)
    private String password;

    private MysqlEventParser controller;

    public Binlog(Map config) {
        super(config);
    }

    @Override
    public void prepare() {
        logger.info("binlog prepare started..");
        controller = new MysqlEventParser();
        controller.setConnectionCharset(Charset.forName("UTF-8"));
        controller.setSlaveId(slaveId);
        controller.setDetectingEnable(false);
        controller.setMasterInfo(new AuthenticationInfo(new InetSocketAddress(host, port), username, password));
        controller.setEnableTsdb(true);
        controller.setDestination("example");
        controller.setParallel(true);
        controller.setParallelBufferSize(256);
        controller.setParallelThreadSize(2);
        controller.setIsGTIDMode(false);

        controller.setEventSink(new BinlogEventSink(this));

        controller.setLogPositionManager(new AbstractLogPositionManager() {

            @Override
            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }

            @Override
            public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
                System.out.println(logPosition);
            }
        });

        logger.info("binlog prepare ended..");
    }

    @Override
    public void emit() {
        logger.info("binlog emit started...");
        controller.start();
        logger.info("binlog emit ended...");
    }

    @Override
    public void release() {
        if(controller != null) {
            controller.stop();
        }
    }

}
