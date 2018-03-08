## Hangout output plugin : Clickhouse

* Author: rickyHuo
* Homepage: https://github.com/RickyHuo/hangout-output-clickhouse
* Version: 0.0.5

### Description

使用Hangout将数据清洗写入ClickHouse

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [bulk_size](#bulk_size-list) | int | no | 1000 |
| [database](#database-string) | string | yes | - |
| [fields](#fields-list) | list | yes | - |
| [format](#format-string) | string | no | TabSeparated |
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

##### format [string]

数据插入格式[Format Introduction](https://clickhouse.yandex/docs/en/formats/)

当前支持`TabSeparated`以及`JSONEachRow`

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
        replace_include_fields: ['_ping_big']
        bulk_size: 500
```

> 使用默认的`TabSeparated`将fields中对应的字段写入ClickHouse，且对`_ping_big`字段中的单引号进行转义

```
outputs:
    - com.sina.bip.hangout.outputs.Clickhouse:
        host: clickhouse.bip.sina.com.cn:8123
        username: user
        password: passwd
        database: apm
        format: JSONEachRow
        table: apm_netdiagno
        bulk_size: 500
```
> 使用`JSONEachRow`将数据写入ClickHouse，使用时务必保证清洗后的数据没有多余的字段