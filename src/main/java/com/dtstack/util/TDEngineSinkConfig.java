package com.dtstack.util;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

public class TDEngineSinkConfig implements Serializable {
    /*
    joyce,
    2024/10/31
    */
    private static final long serialVersionUID = 7235330921548536527L;
    private ParameterTool parameterTool;

    private String url;
    /**
     * TDengine 库名
     */
    // @Value("${spring.tdengine.database}")
    private String dbname = "dbname";

    /**
     * TDengine 用户名
     */
    // @Value("${spring.tdengine.username}")
    private String user = "username";
    /**
     * TDengine 密码
     */
    // @Value("${spring.tdengine.password}")
    private String password = "password";

    /**
     * driverClassName
     */
    // @Value("${spring.tdengine.driverClassName}")
    private String driverClassName = "com.taosdata.jdbc.rs.RestfulDriver";
    //rest api 方式写入
    // String driver = "com.taosdata.jdbc.rs.RestfulDriver";
    //原生tdengine driver
    // String originalDriver = "com.taosdata.jdbc.TSDBDriver";

    // @Value("${spring.tdengine.timezone}")
    private String timezone = "UTC-8";

    // @Value("${spring.tdengine.charset}")
    private String charset = "utf-8";

    private String locale = "en_US.UTF-8";

    // @Value("${spring.tdengine.connect-timeout}")
    private long connectTimeout = 5;

    // @Value("${spring.tdengine.read-timeout}")
    private long readTimeout = 5;

    // @Value("${spring.tdengine.write-timeout}")
    private long writeTimeout = 5;

    private boolean useSSL = false;

    public String getJdbcUrl() {

        String jdbcUrl =  this.url + "/" + this.dbname + "?user=" + this.user + "&password="
                + this.password;
        jdbcUrl += "&rewriteBatchedStatements=true";
        return jdbcUrl;
    }

    public String getUrl() {
        return url;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public TDEngineSinkConfig(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
        url = parameterTool.get("tdengine.url");
        dbname = parameterTool.get("tdengine.dbname");

        user = parameterTool.get("tdengine.user");
        password = parameterTool.get("tdengine.password");
        //driverClassName = parameterTool.get("tdengine.driverClassName");
        driverClassName = "com.taosdata.jdbc.rs.RestfulDriver";//parameterTool.get("com.taosdata.jdbc.TSDBDriver");
        timezone = parameterTool.get("tdengine.timezone");
        locale = parameterTool.get("tdengine.locale");
        connectTimeout = parameterTool.getInt("tdengine.connectTimeout", 5);
        readTimeout = parameterTool.getInt("tdengine.readTimeout", 5);
        writeTimeout = parameterTool.getInt("tdengine.writeTimeout", 5);
        useSSL = parameterTool.getBoolean("tdengine.useSSL", false);
    }

    public String getCharset() {
        return charset;
    }
}