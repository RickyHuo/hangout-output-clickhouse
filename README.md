# hangout-output-plugins-clickhouse

> hangout-out-plugins-clickhouse base on hangout-0.3.0

## Usage




1. 将打包好的jar包(hangout-filters-reverse-0.1.jar)放到hangout的modules文件夹下

2.
```
outputs:
    - com.sina.bip.hangout.outputs.Clickhouse:
        host: clickhouse.bip.sina.com.cn:8123
        database: apm
        table: apm_netdiagno
        fields: ['_device_id', '_ping_small', '_domain', '_traceroute', '_ping_big', 'date', 'ts', '_snet']
        bulk_size: 500
```


## Reference

> [hangout](https://github.com/childe/hangout)

> [hangout-filter-reverse](https://github.com/childe/hangout-filter-reverse)
