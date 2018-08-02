# ClickHouse Format Performance TEST of JDBC

## 前言

[Hangout-output-Clickhouse](https://github.com/RickyHuo/hangout-output-clickhouse)目前支持3种形式的数据插入，[Values](https://clickhouse.yandex/docs/en/formats/values/)、[JSONEachRow](https://clickhouse.yandex/docs/en/formats/jsoneachrow/)以及[TabSeparated](https://clickhouse.yandex/docs/en/formats/tabseparated/)。这三种方式最终插入SQL如下

1. Values
```
insert into db.table (date, datetime, domian, uri, http_code) values ('2018-03-18', '2018-03-19 10:44:57', 'sina.com.cn', '/sports', 200), ('2018-03-18', '2018-03-19 10:44:57', 'sina.com.cn', '/finance', 403)
```

2. JSONEachRow
```
insert into db.table format JSONEachRow {"date":"2018-03-18", "datetime": "2018-03-19 10:44:57", "domain":"sina.com.cn", "uri": "/sports", "http_code":200}{"date":"2018-03-18", "datetime": "2018-03-19 10:44:57", "domain":"sina.com.cn", "uri": "/finance", "http_code":403}
```

3. TabSeparated
```
insert into db.table (date, datetime, domian, uri, http_code) FORMAT TabSeparated
```

#### 测试环境准备

**为了比较三种插入方式的性能，模拟测试场景:**

- ClickHouse
	- 单点（Standalone）
	- Intel(R) Xeon(R) CPU E5-2620 v2 @ 2.10GHz
	- 12 Core
	- HDD

- 数据处理服务器
	- Intel(R) Xeon(R) CPU E5620  @ 2.40GHz
	- 8 Core

- 数据处理信息
	- 2W（Bulk Size）
	- 100（Bulk Number）
	- 0.85KB（Single line）
	- 并发4
	- 测试原始数据条数:2W\*100*4


## 测试结果

![](http://oupfz5jq2.bkt.clouddn.com/18-3-21/66751746.jpg)

## 总结

- Values
性能中等，不需要严格把控字段类型，容易产生插入报错，不推荐使用

- JSONEachRow
性能较差，操作方便，不会产生插入报错

- TabSeparated
性能较好，不产生插入报错，但是需要在配置里严格把控各字段的数据类型，推荐使用
