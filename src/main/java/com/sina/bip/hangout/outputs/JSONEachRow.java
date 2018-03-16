package com.sina.bip.hangout.outputs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class JSONEachRow implements FormatParse {

    private static final Logger log = LogManager.getLogger(JSONEachRow.class);
    private Map config;
    private String host;
    private String database;
    private String table;
    private String jdbcLink;
    private Boolean withCredit;
    private String user;
    private String password;
    public Double fraction;
    private BalancedClickhouseDataSource dataSource;
    private ClickHouseConnectionImpl conn;

    public JSONEachRow(Map config) {
        this.config = config;
    }

    public void prepare() {

        if (this.config.containsKey("fraction")) {
            this.fraction = (Double) this.config.get("fraction");
        } else {
            this.fraction = 1.0;
        }

        if (this.fraction <= 0 || this.fraction > 1) {
            log.error("invalid fraction");
            System.exit(1);
        }

        if (!this.config.containsKey("host")) {
            log.error("hostname must be included in config");
            System.exit(1);
        }
        this.host = (String) this.config.get("host");

        if (!this.config.containsKey("database")) {
            log.error("database must be included in config");
            System.exit(1);
        }
        this.database = (String) this.config.get("database");

        if (!this.config.containsKey("table")) {
            log.error("table must be included in config");
            System.exit(1);
        }
        this.table = (String) this.config.get("table");

        if(this.config.containsKey("username") && this.config.containsKey("password")) {
            this.user = (String) this.config.get("username");
            this.password = (String) this.config.get("password");
            this.withCredit = true;

        } else if (this.config.containsKey("username") || this.config.containsKey("password")) {
            log.warn("username and password must be included in config at same time");
        } else {
            this.withCredit = false;
        }


        // 连接验证
        this.jdbcLink = String.format("jdbc:clickhouse://%s/%s", this.host, this.database);

        ClickHouseProperties properties = new ClickHouseProperties();
        // 避免每次INSERT操作取服务器时间
        properties.setUseServerTimeZone(false);
        this.dataSource = new BalancedClickhouseDataSource(this.jdbcLink, properties);
        if (this.withCredit) {
            ClickHouseProperties withCredentials = properties.withCredentials(this.user, this.password);
            this.dataSource = new BalancedClickhouseDataSource(this.jdbcLink, withCredentials);
        }

        try {
            this.conn = (ClickHouseConnectionImpl) dataSource.getConnection();
        } catch (Exception e) {
            log.error("Cannot connection to datasource");
            log.error(e);
            System.exit(1);
        }

    }

    private String initSql() {

        String init = String.format("insert into %s format JSONEachRow", this.table);
        return init;
    }

    public void bulkInsert(List<Map> events) throws Exception {

        StringBuilder wholeSql = makeUpSql(events);
        try {
            log.trace(wholeSql);
            this.conn.createStatement().execute(wholeSql.toString());
        } catch (SQLException e) {
            log.error(wholeSql.toString());
            log.error(e);
        } catch (Exception e) {
            log.error(e);
        }
    }

    private StringBuilder makeUpSql(List<Map> events) {
        StringBuilder sqls = new StringBuilder(this.initSql());
        for(Map e: events) {
            sqls.append(JSONObject.toJSONString(e));
        }
        return sqls;
    }
}
