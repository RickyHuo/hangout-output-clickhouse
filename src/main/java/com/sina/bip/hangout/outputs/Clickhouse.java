package com.sina.bip.hangout.outputs;

import java.sql.SQLException;
import java.util.*;

import com.ctrip.ops.sysdev.baseplugin.BaseOutput;
import com.ctrip.ops.sysdev.render.TemplateRender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

/**
 * Created by RickyHuo on 2017/09/23.
 */


public class Clickhouse extends BaseOutput {

    private static List<String> formats = Arrays.asList("TabSeparated", "JSONEachRow");
    private static final Logger log = LogManager.getLogger(Clickhouse.class);
    private final static int BULKSIZE = 1000;

    private int bulkNum = 0;
    private String host;
    private String database;
    private String table;
    private String jdbcLink;
    private String format;
    private List<String> fields;
    private List<String> replace_include_fields;
    private List<String> replace_exclude_fields;
    private int bulkSize;
    private String preSql = initSql();
    private List<Map> events;
    private Map<String, String> schema;
    private Boolean withCredit;
    private String user;
    private String password;
    private BalancedClickhouseDataSource dataSource;
    private ClickHouseConnectionImpl conn;
    private Map<String, TemplateRender> templateRenderMap;

    public Clickhouse(Map config) { super (config); }

    protected void prepare() {
        this.events = new ArrayList<>();
        this.templateRenderMap = new HashMap<>();

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

        if (this.config.containsKey("bulk_size")) {
            this.bulkSize = (Integer) this.config.get("bulk_size");
        } else {
            this.bulkSize = BULKSIZE;
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
            log.error("input table is not vaild");
            System.exit(1);
        }


        if (this.config.containsKey("format")) {
            this.format = (String) this.config.get("format");
            if (!this.formats.contains(this.format)) {
                log.error("Not support format: " + this.format);
                System.exit(1);
            }
        } else {
            this.format = "TabSeparated";
        }

        if (this.format.equals("TabSeparated")) {

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

    }

    private String initSql() {

        if (this.format.equals("TabSeparated")) {
            List<String> realFields = new ArrayList<>();
            for(String field: fields) {
                realFields.add(ClickhouseUtils.realField(field));
            }

            String init = String.format("insert into %s (%s) values", this.table, String.join(" ,", realFields));
            log.debug("init sql: "+ init);
            return init;
        } else {
            String init = String.format("insert into %s format JSONEachRow ", this.table);
            log.debug("init sql: "+ init);
            return init;
        }
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

        if ((this.format.equals("TabSeparated"))) {
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
        } else {
            for(Map e: events) {
                sqls.append(JSONObject.toJSONString(e));
            }
        }
        return sqls;
    }

    private void bulkInsert(List<Map> events) throws Exception {

        log.debug("make up SQL start, number: " + this.bulkNum);
        StringBuilder sqls = makeUpSql(events);
        log.debug("make up SQL end, number: " + this.bulkNum);

        try {
            log.trace(sqls);
            this.conn.createStatement().execute(sqls.toString());
        } catch (SQLException e) {
            log.error(e);
            log.error(sqls.toString());

//            for (int i = 0; i < this.events.size(); i++) {
//                log.debug(events.get(i));
//            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    private void eventInsert(Map event) throws Exception {
        eventInsert(event, this.bulkSize);
    }

    private void eventInsert(Map event, int eventSize) throws Exception {

        this.events.add(event);
        if(this.events.size() == eventSize) {
            log.info("Insert bulk start, number: " + this.bulkNum);
            bulkInsert(this.events);
            log.info("Insert bulk end, number: " + this.bulkNum);
            this.events.clear();
            this.bulkNum += 1;
        }
    }

    protected void emit(Map event) {
        try {
            eventInsert(event);
        } catch (Exception e) {
            log.error(e);
            log.warn("insert error");
        }
    }

    public void shutdown() {
        try {
            bulkInsert(this.events);
        } catch (Exception e) {
            log.info("failed to bulk events before shutdown");
            log.debug(e);
        }
    }
}
