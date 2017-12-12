package com.sina.bip.hangout.outputs;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class ClickhouseCredentialsTest {

    public static void main(String args[]) throws Exception {
        String jdbc = "jdbc:clickhouse://ck31.mars.grid.com.cn:8123/ck_test";


        ClickHouseProperties properties = new ClickHouseProperties();
        ClickHouseProperties withCredentials = properties.withCredentials("default", "default");
        BalancedClickhouseDataSource balanced = new BalancedClickhouseDataSource(jdbc, withCredentials);
        Connection conn = balanced.getConnection();
        System.out.println(conn.createStatement().executeQuery("show tables"));
    }
}
