/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.jlogstash.inputs;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.exception.InitializeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
public class Jdbc extends BaseInput {

    private static final Logger logger = LoggerFactory.getLogger(JdbcClientHelper.class);

    @Required(required = true)
    private String jdbc_connection_string;

    @Required(required = true)
    private String jdbc_driver_class;

    @Required(required = true)
    private String jdbc_driver_library;

    private Integer jdbc_fetch_size;

    @Required(required = true)
    private String jdbc_user;

    @Required(required = true)
    private String jdbc_password;

    @Required(required = true)
    private String statement;

    private String parameters;

    private volatile boolean stop;

    private Connection connection;

    @SuppressWarnings("rawtypes")
    public Jdbc(Map config) {
        super(config);
    }

    @Override
    public void prepare() {
        connection = initConn(); // 创建连接
    }

    @Override
    public void emit() {

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(statement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (jdbc_fetch_size != null) {
                preparedStatement.setFetchSize(jdbc_fetch_size);
            }

            resultSet = preparedStatement.executeQuery();

            // 获取列名
            List<String> columnNameList = new ArrayList<String>();
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int count = rsmd.getColumnCount();
            for (int i = 1; i <= count; i++) {
                columnNameList.add(rsmd.getColumnName(i));
            }

            // 逐行读取
            while (!stop && resultSet.next()) {
                Map<String, Object> rowMap = new HashMap<String, Object>();
                for (String columnName : columnNameList) {
                    rowMap.put(columnName, resultSet.getObject(columnName));
                }

                process(rowMap);
            }
        } catch (SQLException e) {
            logger.error("read failed", e);
        } finally {
            JdbcClientHelper.releaseConnection(connection, preparedStatement, resultSet);
        }
    }

    @Override
    public void release() {
        stop();
        connection = null;
    }

    private void stop() {
        stop = true;
    }

    private Connection initConn()  {
        ConnectionConfig connectionConfig = new ConnectionConfig();
        connectionConfig.setJdbc_connection_string(jdbc_connection_string);
        connectionConfig.setJdbc_driver_class(jdbc_driver_class);
        connectionConfig.setJdbc_driver_library(jdbc_driver_library);
        connectionConfig.setJdbc_user(jdbc_user);
        connectionConfig.setJdbc_password(jdbc_password);

        try {
            return JdbcClientHelper.getConnection(connectionConfig);
        } catch (Exception e) {
            throw new InitializeException("get jdbc connection failed", e);
        }
    }

}
