## Hangout output plugin : Clickhouse

* Author: rickyHuo
* Homepage: https://github.com/RickyHuo/hangout-output-clickhouse
* Version: 1.0.0

### Description

使用Hangout将数据清洗写入ClickHouse

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [host](#host-string) | string | yes | - |
| [database](#database-string) | string | yes | - |
| [table](#table-string) | string | yes | - |
| [fields](#fields-list) | list | yes | - |
| [bulk_size](#bulk_size-list) | int | no | 1000 |
| [replace_include_fields](#replace_include_fields-list) | list | no | - |
| [replace_exclude_fields](#replace_exclude_fields-list) | list | no | - |

##### host [string]

ClickHouse cluster host

##### database [string]

database

##### table [string]

table

##### fields [list]

table fields， 必须和Hangout清洗后的字段保持一致

##### bulk_size [string]

批次写入量，默认为1000

##### replace_include_fields [list]

需要执行替换'字符操作的字段列表

##### replace_exclude_fields [list]

不需要执行替换'字符操作的字段列表

### Examples

```
outputs:
    - com.sina.bip.hangout.outputs.Clickhouse:
        host: clickhouse.bip.sina.com.cn:8123
        database: apm
        table: apm_netdiagno
        fields: ['_device_id', '_ping_small', '_domain', '_traceroute', '_ping_big', 'date', 'ts', '_snet']
        bulk_size: 500
```

> 将fields中对应的字段写入ClickHouse

