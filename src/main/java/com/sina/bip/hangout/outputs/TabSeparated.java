package com.sina.bip.hangout.outputs;

import com.ctrip.ops.sysdev.render.TemplateRender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TabSeparated implements FormatParse {

    private static final Logger log = LogManager.getLogger(TabSeparated.class);
    private Map config;
    private String host;
    private String database;
    private String table;
    private String jdbcLink;
    private List<String> fields;
    private Map<String, String> schema;
    private Boolean withCredit;
    private String user;
    private String password;
    private Double fraction;
    private BalancedClickhouseDataSource dataSource;
    private ClickHouseConnectionImpl conn;
    private Map<String, TemplateRender> templateRenderMap;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Pattern lowCardinalityPattern = Pattern.compile("LowCardinality\\((.*)\\)");


    public TabSeparated(Map config) {
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

        if (this.config.containsKey("username") && this.config.containsKey("password")) {
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
//        properties.setUseServerTimeZone(false);
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

        for (String field : fields) {
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
        for (String field : this.fields) {
            realFields.add(ClickhouseUtils.realField(field));
        }

        String init = String.format("insert into %s.%s (%s) values (%s)", this.database,
                this.table,
                String.join(", ", realFields),
                ClickhouseUtils.tabSeparatedPreSql(this.fields.size()));

        log.debug("init sql: " + init);
        return init;
    }

    private void renderStatement(int index, String fieldType, Object fieldValue, PreparedStatement statement) throws Exception {
        switch (fieldType) {
            case "Int8":
            case "Int16":
            case "Int32":
            case "UInt8":
            case "UInt16":
                if (fieldValue != null) {
                    try {
                        int v = ((Number) fieldValue).intValue();
                        statement.setInt(index, v);

                    } catch (Exception exp) {
                        String msg = String.format("Cannot Convert %s %s to integer, render default. Field index is %s", fieldValue.getClass(), fieldValue, index);
                        log.warn(msg);
                        log.error(exp);
                        statement.setInt(index, 0);
                    }
                } else {
                    statement.setInt(index, 0);
                }
                break;
            case "UInt64":
            case "Int64":
            case "UInt32":
                if (fieldValue != null) {
                    try {
                        long v = ((Number) fieldValue).longValue();
                        statement.setLong(index, v);

                    } catch (Exception exp) {
                        String msg = String.format("Cannot Convert %s %s to long, render default", fieldValue.getClass(), fieldValue);
                        log.warn(msg);
                        log.error(exp);
                        statement.setInt(index, 0);
                    }
                } else {
                    statement.setInt(index, 0);
                }
                break;
            case "String":
                if (fieldValue != null) {
                    statement.setString(index, fieldValue.toString());
                } else {
                    statement.setString(index, "");
                }
                break;
            case "DateTime":
                if (fieldValue != null) {
                    if (fieldValue.equals(0) || fieldValue.equals("0")) {
                        statement.setString(index, "0000-00-00 00:00:00");
                    } else {
                        statement.setString(index, fieldValue.toString());
                    }
                } else {
                    statement.setString(index, this.datetimeFormat.format(System.currentTimeMillis()));
                }
                break;
            case "Date":
                if (fieldValue != null) {
                    try {
                        this.dateFormat.parse(fieldValue.toString());
                        statement.setString(index, fieldValue.toString());
                    } catch (Exception exp) {
                        log.warn(exp);
                        statement.setString(index, this.dateFormat.format(System.currentTimeMillis()));
                    }
                } else {
                    statement.setString(index, this.dateFormat.format(System.currentTimeMillis()));
                }
                break;
            case "Float32":
                if (fieldValue != null) {
                    try {
                        float v = ((Number) fieldValue).floatValue();
                        statement.setFloat(index, v);
                    } catch (Exception exp) {
                        String msg = String.format("Cannot Convert %s %s to float, render default", fieldValue.getClass(), fieldValue);
                        log.warn(msg);
                        log.error(exp);
                        statement.setFloat(index, 0f);
                    }
                } else {
                    statement.setFloat(index, 0f);
                }
                break;
            case "Array(String)":
                if (fieldValue != null) {
                    List<String> v = (List) fieldValue;
                    String[] array = v.toArray(new String[v.size()]);
                    statement.setArray(index, this.conn.createArrayOf("string", array));
                } else {
                    statement.setArray(index, this.conn.createArrayOf("string", new String[1]));
                }
                break;
            default:
                Matcher m = lowCardinalityPattern.matcher(fieldType);
                if (m.find()) {
//                    System.out.print(m.groupCount());
                    renderStatement(index, m.group(1), fieldValue, statement);
                } else {
                    renderStatement(index, "String", fieldValue, statement);
                }
        }
    }

    public void bulkInsert(List<Map> events) throws Exception {

        PreparedStatement statement = this.conn.prepareStatement(this.initSql());
        int size = fields.size();
        for (Map e : events) {
            for (int i = 0; i < size; i++) {
                String field = fields.get(i);
                String fieldType = this.schema.get(ClickhouseUtils.realField(field));
                Object fieldValue = this.templateRenderMap.get(field).render(e);
                renderStatement(i + 1, fieldType, fieldValue, statement);
            }
            statement.addBatch();
        }
        statement.executeBatch();
//        statement.close();
    }
}
