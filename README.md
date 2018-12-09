## Hangout output plugin : Clickhouse

* Author: rickyHuo
* Homepage: https://github.com/RickyHuo/hangout-output-clickhouse
* Version: 0.0.7

### Description

使用Hangout将数据清洗写入ClickHouse，**兼容hangout-dist-0.3.0-release**， 0.4.0版本hangout兼容性未知。

如果线上有现成的Spark环境，可以使用[Waterdrop](https://github.com/InterestingLab/waterdrop)接入数据，支持Kafka、HDFS、Hive等。

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [bulk_size](#bulk_size-number) | int | no | 1000 |
| [database](#database-string) | string | yes | - |
| [fields](#fields-list) | list | yes | - |
| [format](#format-string) | string | no | TabSeparated |
| [host](#host-string) | string | yes | - |
| [table](#table-string) | string | yes | - |
| [password](#password-string) | string | yes | - |
| [username](#username-string) | string | yes | - |

##### bulk_size [number]

批次写入量，默认为1000，**当且仅当数据条数达到bulk_size时，才会触发写入操作。**

dev版本正在开发时间触发器，敬请期待...

##### database [string]

ClickHouse database.

##### fields [list]

期望写入ClickHouse的数据字段。


##### format [string]

数据插入格式[ClickHouse Format Introduction](https://clickhouse.yandex/docs/en/formats/)

当前支持`JSONEachRow`以及`TabSeparated`。

[JDBC Format Performance TEST](./docs/jdbc_format_performance.md)

##### host [string]

ClickHouse集群地址，多个以逗号","隔开。

##### table [string]

ClickHouse table name.

##### username [string]

ClickHouse withCredit username.

##### password [string]

ClickHouse withCredit password.

### Examples

```
outputs:
    - com.sina.bip.hangout.outputs.Clickhouse:
        host: localhost:8123
        username: user
        password: passwd
        database: apm
        table: apm_netdiagno
        fields: ['_device_id', '_ping_small', '_domain', '_traceroute', '_ping_big', 'date', 'ts', '_snet']
        bulk_size: 500
```

> 使用`Tabseparated`(default)将fields中对应的字段写入ClickHouse。

```
outputs:
    - com.sina.bip.hangout.outputs.Clickhouse:
        host: localhost:8123
        username: user
        password: passwd
        database: apm
        format: JSONEachRow
        table: apm_netdiagno
        bulk_size: 500
```
> 使用`JSONEachRow`将数据写入ClickHouse，使用时务必保证清洗后的数据没有多余的字段且与表结构对应。使用`JSONEachRow`则不需要配置`fields`参数。


### Tips

在写入ClickHouse之前，Date和DateTime类型的字段需要转换为指定格式。

- Date

    `yyyy-MM-dd`

- DateTime

    - `yyyy-MM-dd HH:mm:ss`
    - `UNIX` 例如：1533535518
    
### Supported data types

* UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
* Float32, Float64
* Date
* DateTime
* Array(String)
