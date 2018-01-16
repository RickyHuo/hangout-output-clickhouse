## Hangout output plugin : Clickhouse

* Author: rickyHuo
* Homepage: https://github.com/RickyHuo/hangout-output-clickhouse
* Version: 0.0.3

### Description

使用Hangout将数据清洗写入ClickHouse

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [bulk_size](#bulk_size-list) | int | no | 1000 |
| [database](#database-string) | string | yes | - |
| [fields](#fields-list) | list | yes | - |
| [host](#host-string) | string | yes | - |
| [replace_include_fields](#replace_include_fields-list) | list | no | - |
| [replace_exclude_fields](#replace_exclude_fields-list) | list | no | - |
| [table](#table-string) | string | yes | - |
| [username](#username-string) | string | yes | - |
| [password](#password-string) | string | yes | - |

##### bulk_size [string]

批次写入量，默认为1000

##### database [string]

database

##### fields [list]

table fields， 必须和Hangout清洗后的字段保持一致

##### host [string]

ClickHouse cluster host

##### replace_include_fields [list]

需要执行替换'字符操作的字段列表

##### replace_exclude_fields [list]

不需要执行替换'字符操作的字段列表

##### table [string]

table

##### username [string]

ClickHouse withCredit username

##### password [string]

ClickHouse withCredit password

### Examples

```
outputs:
    - com.sina.bip.hangout.outputs.Clickhouse:
        host: clickhouse.bip.sina.com.cn:8123
        username: user
        password: passwd
        database: apm
        table: apm_netdiagno
        fields: ['_device_id', '_ping_small', '_domain', '_traceroute', '_ping_big', 'date', 'ts', '_snet']
        bulk_size: 500
```

> 将fields中对应的字段写入ClickHouse

