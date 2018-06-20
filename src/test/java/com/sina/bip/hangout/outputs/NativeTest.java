package com.sina.bip.hangout.outputs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class NativeTest {
    public static void main(String[] args) throws Exception {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://ck31.mars.grid.sina.com.cn:9000");

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("desc test.dpool_msg");

        while (rs.next()) {
            System.out.println(rs.getInt(1) + "\t" + rs.getLong(2));
        }
    }
}
