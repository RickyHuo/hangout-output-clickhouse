package com.sina.bip.hangout.outputs;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.SimpleDateFormat;

/**
 * Created by huochen on 2017/11/13.
 */

public class ClickhouseUtils {

    public static Map<String, String> getSchema(ClickHouseConnectionImpl connection, String table) throws SQLException {

        String sql = String.format("desc %s", table);
        ResultSet resultSet = connection.createStatement().executeQuery(sql);
        Map schema = new HashMap<String, String>();
        while(resultSet.next()) {

            schema.put(resultSet.getString(1), resultSet.getString(2));
        }
        return schema;
    }

    public static String renderDefault(String dataType) {

        if (dataType.equals("String"))
            return "''";
        else if (dataType.equals("Date")) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            return String.format("'%s'", dateFormat.format(new Date()));
        }
        else if (dataType.equals("DataTime")) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return String.format("'%s'", dateFormat.format(new Date()));
        } else
            return "";
    }

    public static String realField(String field) {

        final Pattern p = Pattern.compile("\\[(\\S+?)\\]+");
        ArrayList<String> fields = new ArrayList();
        Matcher m = p.matcher(field);
        while (m.find()) {
            String a = m.group();
            fields.add(a.substring(1, a.length() - 1));
        }

        int size = fields.size();
        if (size == 0) {
            return field;
        } else {
            return fields.get(size - 1);
        }
    }
}
