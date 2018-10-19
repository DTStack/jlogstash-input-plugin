package com.dtstack.jlogstash.inputs;


import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;


public class BinlogJournalValidator {

    private static final Logger logger = LoggerFactory.getLogger(BinlogJournalValidator.class);

    private String host;

    private int port;

    private String user;

    private String pass;

    public BinlogJournalValidator(String host, int port, String user, String pass) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
    }

    public boolean check(String journalName) {
        return listJournals().contains(journalName);
    }

    public List<String> listJournals() {
        List<String> journalList = new ArrayList<>();
        MysqlConnection conn = new MysqlConnection(new InetSocketAddress(host, port), user, pass, (byte) 33, null);
        try {
            conn.connect();
            ResultSetPacket resultSetPacket = conn.query("show binary logs");
            List<String> fieldValues = resultSetPacket.getFieldValues();
            for(int i = 0; i < fieldValues.size(); ++i) {
                if(i % 2 == 0) {
                    journalList.add(fieldValues.get(i));
                }
            }
            logger.info("collect journals: " + journalList);
        } catch (IOException e) {
            logger.error("Error occured: " + e.getMessage());
        } finally {
            if(conn != null) {
                try {
                    conn.disconnect();
                } catch (IOException e) {
                    logger.error("Error occured while disconnect mysqlconnection: " + e.getMessage());
                }
            }
        }
        return journalList;
    }

    public static void main(String[] args) {
        BinlogJournalValidator validator = new BinlogJournalValidator("rdos1", 3306, "canal", "canal");
        validator.listJournals();
    }
}
