package com.sina.bip.hangout.outputs;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.ctrip.ops.sysdev.baseplugin.BaseOutput;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

/**
 * Created by huochen on 2017/09/23.
 */
public class Clickhouse extends BaseOutput{
    private final static int BULKSIZE = 1000;

    private String host;
    private String database;
    private String table;
    private String jdbcLink;
    private List<String> fields;
    private List<String> replace_fields = new ArrayList<String>();
    private int bulkSize;
    private String preSql = initSql();
    private List<Map> events;
    private StringBuilder sql = new StringBuilder(preSql);
    private Map<String, String> schema;

    public Clickhouse(Map config) { super (config); }

    protected void prepare() {
        this.events = new ArrayList<Map>();

        if(!this.config.containsKey("host")) {
            System.out.println("hostname must be included in config");
            System.exit(1);
        }
        this.host = (String) this.config.get("host");

        if(!this.config.containsKey("database")) {
            System.out.println("database must be included in config");
            System.exit(1);
        }
        this.database = (String) this.config.get("database");

        if(!this.config.containsKey("table")) {
            System.out.println("table must be included in config");
            System.exit(1);
        }
        this.table = (String) this.config.get("table");

        if(this.config.containsKey("bulk_size")) {
            this.bulkSize = (Integer) this.config.get("bulk_size");
        } else {
            this.bulkSize = BULKSIZE;
        }

        if(this.config.containsKey("fields")) {
            this.fields = (List<String>) this.config.get("fields");
        } else {
            System.out.println("fields must be included in config");
        }

        if(this.config.containsKey("replace_fields")) {
            this.replace_fields = (List<String>) this.config.get("replace_fields");
        }
        this.jdbcLink = String.format("jdbc:clickhouse://%s/%s", this.host, this.database);

        // ClickHouseDataSource 不支持逗号","分割的多个节点

        String databaseSource = String.format("jdbc:clickhouse://%s/%s", this.host.split(",")[0], this.database);
        ClickHouseDataSource dataSource = new ClickHouseDataSource(databaseSource);
        try {
            this.schema = ClickhouseUtils.getSchema(dataSource, this.table);
        } catch (SQLException e) {
            System.out.println("table is not vaild");
            System.exit(1);
        }

        for (String field: fields) {
            if (!this.schema.containsKey(field)) {
                String msg = String.format("table [%s] doesn't contain field '%s'", this.table, field);
                System.out.println(msg);
                System.exit(1);
            }
        }
    }

    protected String initSql() {
        String init = String.format("insert into %s (%s) values", this.table, String.join(" ,", this.fields));
        return init;
    }

    protected void bulkInsert(Map event) throws Exception{

        this.events.add(event);
        if(this.events.size() == this.bulkSize) {
            StringBuilder sqls = new StringBuilder(preSql);
            for (int i = 0; i < this.events.size(); i++) {
                Map e = events.get(i);
                String value = "(";
                for (int j =0; j < fields.size(); j++) {
                    String field = fields.get(j);
                    // 判断元数据中是否有对应的key

                    if (e.containsKey(field)) {
                        if (e.get(field) instanceof String) {
                            String fieldValue = e.get(field).toString();
                            if (replace_fields.contains(field)) {
                                if (!(fieldValue.indexOf("'") > 0)){
                                    value += "'" + e.get(field) + "'";
                                } else {
                                    if (fieldValue.indexOf("\\'") > 0)
                                        value += "'" + fieldValue.replace("'", "\\'").replace("\\\\'", "\\\\\\'") + "'";
                                    else
                                        value += "'" + fieldValue.replace("'", "\\'") + "'";
                                }
                            } else {
                                value += "'" + fieldValue + "'";
                            }
                        } else {
                            if (e.get(field) == null){
                                value += "''";
                            } else {
                                value += e.get(field);
                            }
                        }
                    } else {
                        value += ClickhouseUtils.renderDefault(this.schema.get(field));
                    }
                    if(j != fields.size() -1 ) {
                        value += ",";
                    }
                }
                value += ")";
                sqls.append(value);
            }
            ClickHouseProperties properties = new ClickHouseProperties();
            BalancedClickhouseDataSource balanced = new BalancedClickhouseDataSource(this.jdbcLink, properties);

            Connection conn = balanced.getConnection();
            try {
                conn.createStatement().execute(sqls.toString());
            } catch (SQLException e){
                System.out.println(sqls.toString());
                System.out.println(e.toString());
                for (int i = 0; i < this.events.size(); i++) {
                    System.out.println(events.get(i));
                }
                // System.out.println("insert error");
            } catch (Exception e) {
                System.out.println(e.toString());
                System.out.println("error");
            }
            conn.close();
            this.events.clear();
        }
    }

    protected void emit(Map event) {
        try {
            bulkInsert(event);
        } catch (Exception e) {
            System.out.println(e.toString());
            System.out.println("insert error");
        }
    }

    public void shutdown() {
    }
}
