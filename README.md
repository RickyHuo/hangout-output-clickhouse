# hangout-output-plugins-clickhouse

`hangout`输出写入`Clickhouse`的插件

## Sample

```
outputs:
    - com.sina.bip.hangout.outputs.Clickhouse:
        host: clickhouse.bip.sina.com.cn:8123
        database: apm
        table: apm_netdiagno
        fields: ['_device_id', '_ping_small', '_domain', '_traceroute', '_ping_big', 'date', 'ts', '_snet']
        bulk_size: 500
```

## 说明
> `fields`列表字段需要与`clickhouse`中数据表的字段保持一致， 如果不一致会导致插入报错， 可以使用*filters.Rename*对字段重命名