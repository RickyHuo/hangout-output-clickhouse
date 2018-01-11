package com.sina.bip.hangout.outputs;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.ops.sysdev.baseplugin.BaseOutput;
import com.ctrip.ops.sysdev.render.TemplateRender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

/**
 * Created by huochen on 2017/09/23.
 */


public class Clickhouse extends BaseOutput {

    private static final Logger log = LogManager.getLogger(Clickhouse.class);
    private final static int BULKSIZE = 1000;

    private String host;
    private String database;
    private String table;
    private String jdbcLink;
    private List<String> fields;
    private List<String> replace_include_fields;
    private List<String> replace_exclude_fields;
    private int bulkSize;
    private String preSql = initSql();
    private List<Map> events;
    private StringBuilder sql = new StringBuilder(preSql);
    private Map<String, String> schema;
    private Boolean withCredit;
    private String user;
    private String password;
    private Map<String, TemplateRender> templateRenderMap;

    public Clickhouse(Map config) { super (config); }

    protected void prepare() {
        this.events = new ArrayList<Map>();

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

        if(this.config.containsKey("user") && this.config.containsKey("password")) {
            this.user = (String) this.config.get("user");
            this.password = (String) this.config.get("password");
            this.withCredit = true;

        } else if (this.config.containsKey("user") || this.config.containsKey("password")) {
            log.warn("user and password must be included in config at same time");
        } else {
            this.withCredit = false;
        }

        if (this.config.containsKey("bulk_size")) {
            this.bulkSize = (Integer) this.config.get("bulk_size");
        } else {
            this.bulkSize = BULKSIZE;
        }

        if (this.config.containsKey("fields")) {
            this.fields = (List<String>) this.config.get("fields");
        } else {
            log.error("fields must be included in config");
            System.exit(1);
        }

        if (this.config.containsKey("replace_include_fields")) {
            this.replace_include_fields = (List<String>) this.config.get("replace_include_fields");
        }

        if (this.config.containsKey("replace_exclude_fields")) {
            this.replace_exclude_fields = (List<String>) this.config.get("replace_exclude_fields");
        }

        if (this.config.containsKey("replace_include_fields") && this.config.containsKey("replace_exclude_fields")) {
            log.error("Replace_include_fields and replace_exclude_fields exist at the same time, " +
                    "please use one of them.");
            System.exit(1);
        }

        this.jdbcLink = String.format("jdbc:clickhouse://%s/%s", this.host, this.database);

        // ClickHouseDataSource 不支持逗号","分割的多个节点
        String databaseSource = String.format("jdbc:clickhouse://%s/%s", this.host.split(",")[0], this.database);
        ClickHouseProperties properties = new ClickHouseProperties();


        ClickHouseDataSource dataSource = new ClickHouseDataSource(databaseSource);
        if (this.withCredit) {
            ClickHouseProperties withCredentials = properties.withCredentials(this.user, this.password);
            dataSource = new ClickHouseDataSource(databaseSource, withCredentials);
        }
        try {
            this.schema = ClickhouseUtils.getSchema(dataSource, this.table);
        } catch (SQLException e) {
            log.error("input table is not vaild");
            System.exit(1);
        }

        this.templateRenderMap = new HashMap<>();
        for (String field: fields) {
            if (!this.schema.containsKey(ClickhouseUtils.realField(field))) {
                String msg = String.format("table [%s] doesn't contain field '%s'", this.table, field);
                log.error(msg);
                System.exit(1);
            }
            try {
                this.templateRenderMap.put(field, TemplateRender.getRender(field, false));
            } catch (Exception e) {
                String msg = String.format("cannot get templateRender of field [%s]", field);
                log.warn(msg);
            }
        }

    }

    private String initSql() {

        List<String> realFields = new ArrayList<>();
        for(String field: fields) {
            realFields.add(ClickhouseUtils.realField(field));
        }

        String init = String.format("insert into %s (%s) values", this.table, String.join(" ,", realFields));
        log.debug("init sql: "+ init);
        return init;
    }

    private String dealWithQuote(String str) {
        /*
        * 因为Clickhouse SQL语句必须用单引号'， 例如：
        * insert into test.test (date, value) values ('2017-10-29', '23')
        * SQL语句需要将数值中的单引号'转义
        * */

        if (!str.contains("'")) {
            return str;
        } else if (str.indexOf("\\'") > 0) {
//            deal with "\'"
            return str.replace("'", "\\'").replace("\\\\'", "\\\\\\'");
        } else {
            return str.replace("'", "\\'");
        }
    }

    private StringBuilder makeUpSql(List<Map> events) {

        StringBuilder sqls = new StringBuilder(preSql);
        for(Map e: events) {
            StringBuilder value = new StringBuilder("(");
            for(String field: fields) {
                Object fieldValue = this.templateRenderMap.get(field).render(e);
                if (fieldValue != null) {
                    if (fieldValue instanceof String) {
                        if ((this.replace_include_fields != null && this.replace_include_fields.contains(field)) ||
                                (this.replace_exclude_fields != null && !this.replace_exclude_fields.contains(field))) {
                            value.append("'");
                            value.append(dealWithQuote(fieldValue.toString()));
                            value.append("'");
                        } else {
                            value.append("'");
                            value.append(fieldValue.toString());
                            value.append("'");
                        }
                    } else {
                        if (e.get(field) == null){
                            value.append("''");
                        } else {
                            value.append(e.get(field));
                        }
                    }
                } else {
                    value.append(ClickhouseUtils.renderDefault(this.schema.get(ClickhouseUtils.realField(field))));
                }
                if (fields.indexOf(field) != fields.size() -1) {
                    value.append(",");
                }
            }
            value.append(")");
            sqls.append(value);
        }
        return sqls;
    }


    private void bulkInsert(List<Map> events) throws Exception {

        StringBuilder sqls = makeUpSql(events);
        ClickHouseProperties properties = new ClickHouseProperties();

        BalancedClickhouseDataSource balanced = new BalancedClickhouseDataSource(this.jdbcLink, properties);

        if (this.withCredit) {
            ClickHouseProperties withCredentials = properties.withCredentials(this.user, this.password);
            balanced = new BalancedClickhouseDataSource(this.jdbcLink, withCredentials);
        }

        Connection conn = balanced.getConnection();
        try {
            conn.createStatement().execute(sqls.toString());
            conn.close();
        } catch (SQLException e){
            log.error(e.toString());
            log.debug(sqls.toString());

            for (int i = 0; i < this.events.size(); i++) {
                log.debug(events.get(i));
            }
        } catch (Exception e) {
            log.error("error");
        }
        conn.close();
    }

    private void eventInsert(Map event) throws Exception {
        eventInsert(event, this.bulkSize);
    }

    private void eventInsert(Map event, int eventSize) throws Exception {

        this.events.add(event);
        if(this.events.size() == eventSize) {
            bulkInsert(this.events);
            this.events.clear();
        }
    }

    protected void emit(Map event) {
        try {
            eventInsert(event);
        } catch (Exception e) {
            log.error(e.toString());
            log.warn("insert error");
        }
    }

    public void shutdown() {
    }
}
