package com.dtstack.function;

import com.dtstack.util.TDEngineSinkConfig;
import com.taosdata.jdbc.SchemalessWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class TDEngineSink extends RichSinkFunction<String> implements Serializable {
    /*
    joyce,
    2024/10/31
    */
    private static final long serialVersionUID = -9284365084784543L;

    private static final Logger logger = LoggerFactory.getLogger(TDEngineSink.class);
    TDEngineSinkConfig config;
    Connection connection;
    PreparedStatement stmt;

    public TDEngineSink( TDEngineSinkConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            Class.forName(config.getDriverClassName());
            Properties connProps = new Properties();
            connProps.setProperty("charset", this.config.getCharset());
            connProps.setProperty("locale", "en_US.UTF-8");
            connProps.setProperty("timezone", "UTC-8");
            logger.info("init Conn ==>"+connProps);
            connProps.setProperty("batchfetch", "true");
            //原生url
            //jdbc:TAOS://172.16.101.216:16030/demo?user=root&password=password
            //rest api url
            //jdbc:TAOS-RS://172.16.101.216:16041/demo?user=root&password=password
            //测试环境连接信息
            //jdbc:TAOS://172.16.101.216:16030/demo?user=root&password=password
            String jdbcurl = "jdbc:TAOS-RS://172.16.101.216:16041/demo?user=root&password=password";
            connection = DriverManager.getConnection(jdbcurl, connProps);



        }catch (Exception e){
            e.printStackTrace();
            System.out.println("init error ==>"+e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.stmt != null) {
            this.stmt.close();
        }

        if (this.connection != null) {
            this.connection.close();
        }
        logger.info("already close connection and prepareStatement!");
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        super.invoke(value, context);
        logger.info("invoke  获取最后value ==>" +value);
        String sql = "show tables;";
        this.stmt = this.connection.prepareStatement(sql);

        ResultSet rs = stmt.executeQuery();
        if(rs.next()) {
            System.out.println("stmt result ==>"+rs.getString(1)+"\nend");
            logger.info("stmt result ==>"+rs.getString(1)+"\nend");
        }
    }
}