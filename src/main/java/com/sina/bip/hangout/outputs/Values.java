package com.sina.bip.hangout.outputs;

import com.ctrip.ops.sysdev.render.TemplateRender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Deprecated
/*
* It can be replaced by TabSeparated with better performance
*
* */
public class Values implements FormatParse {

    private static final Logger log = LogManager.getLogger(Values.class);
    private Map config;
    private String host;
    private String database;
    private String table;
    private String jdbcLink;
    private List<String> fields;
    private List<String> replace_include_fields;
    private List<String> replace_exclude_fields;
    private Map<String, String> schema;
    private Boolean withCredit;
    private String user;
    private String password;
    private Double fraction;
    private BalancedClickhouseDataSource dataSource;
    private ClickHouseConnectionImpl conn;
    private Map<String, TemplateRender> templateRenderMap;

    public Values(Map config) {
        this.config = config;
    }

    public void prepare() {
        this.templateRenderMap = new HashMap<>();

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

        try {
            this.schema = ClickhouseUtils.getSchema(this.conn, this.table);
        } catch (SQLException e) {
            log.error("input table is not valid");
            System.exit(1);
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

    public String initSql() {

        List<String> realFields = new ArrayList<>();
        for(String field: this.fields) {
            realFields.add(ClickhouseUtils.realField(field));
        }
        String init = String.format("insert into %s (%s) values", this.table, String.join(" ,", realFields));
        log.debug("init sql: " + init);
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

    protected StringBuilder makeUpSql(List<Map> events) {
        StringBuilder wholeSql = new StringBuilder(this.initSql());
        for (Map e: events) {
            StringBuilder value = new StringBuilder("(");
            for(String field: fields) {
                Object fieldValue = this.templateRenderMap.get(field).render(e);
                String fieldType = this.schema.get(ClickhouseUtils.realField(field));
                if (fieldValue != null) {
                    switch (fieldType) {
                        case "String":
                        case "DateTime":
                        case "Date":
                            if ((this.replace_include_fields != null && this.replace_include_fields.contains(field)) ||
                                    (this.replace_exclude_fields != null && !this.replace_exclude_fields.contains(field))) {
                                value.append("'");
                                value.append(dealWithString(fieldValue.toString(), true));
                                value.append("'");
                            } else {
                                value.append("'");
                                value.append(dealWithString(fieldValue.toString(), false));
                                value.append("'");
                            }
                            break;
                        case "Int8":
                        case "Int16":
                        case "Int32":
                        case "Int64":
                        case "UInt8":
                        case "UInt16":
                        case "UInt32":
                        case "UInt64":
                        case "Float32":
                        case "Float64":
                            value.append(e.get(field));
                            break;
                    }
                } else {
                    value.append(ClickhouseUtils.renderDefault(this.schema.get(ClickhouseUtils.realField(field))));
                }

                if (fields.indexOf(field) != fields.size() -1) {
                    value.append(",");
                }
            }
            value.append(")");
            wholeSql.append(value);
        }
        return wholeSql;
    }

    private String dealWithString(String str, Boolean bol) {
        if (bol) {
            return dealWithQuote(str);
        } else {
            return str;
        }
    }

    private String dealWithQuote(String str) {
        /*
        * 因为Values形式ClickHouse SQL语句必须用单引号'， 例如：
        * insert into test.test (date, value) values ('2017-10-29', '23')
        * SQL语句需要将数值中的单引号'转义
        * */

        if (!str.contains("'")) {
            return str;
        } else if (str.indexOf("\\'") > 0) {
        // deal with "\'"
            return str.replace("'", "\\'").replace("\\\\'", "\\\\\\'");
        } else {
            return str.replace("'", "\\'");
        }
    }

}
