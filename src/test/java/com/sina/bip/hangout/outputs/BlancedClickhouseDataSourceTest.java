package com.sina.bip.hangout.outputs;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ru.yandex.clickhouse.ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX;

public class BlancedClickhouseDataSourceTest {
    public static void main(String args[]) throws Exception {

        String h = "localhost:8123";
        String db = "test";
        ClickHouseProperties properties = new ClickHouseProperties();
        //String jdbcLink = String.format("jdbc:clickhouse://%s/%s", h, db);
        String jdbcLink = "jdbc:clickhouse://localhost:8123,localhost1:8123,localhost2:8123/test";

        Pattern URL_TEMPLATE = Pattern.compile(JDBC_CLICKHOUSE_PREFIX + "//([a-zA-Z0-9_:,.]+)(/[a-zA-Z0-9_]+)?");
        Matcher m = URL_TEMPLATE.matcher(jdbcLink);
        if (!m.matches()) {
            throw new IllegalArgumentException("Incorrect url");
        }
        String database = m.group(2);
        if (database == null) {
            database = "";
        }
        String[] hosts = m.group(1).split(",");
        final List<String> result = new ArrayList<String>(hosts.length);
        for (final String host : hosts) {
            result.add(JDBC_CLICKHOUSE_PREFIX + "//" + host + database);
        }

        String JDBC_PREFIX = "jdbc:";
        String strUri = result.get(0).substring(JDBC_PREFIX.length());
        URI uri = new URI(strUri);
        System.out.println(uri.getHost());
    }
}
