package com.sina.bip.hangout.outputs;

import java.util.*;

import com.ctrip.ops.sysdev.baseplugin.BaseOutput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by RickyHuo on 2017/09/23.
 */


public class Clickhouse extends BaseOutput {

    private static final Logger log = LogManager.getLogger(Clickhouse.class);
    private final static int BULKSIZE = 1000;

    private int bulkNum = 0;
    private int bulkSize;
    private List<Map> events;
    private FormatParse formatParse;

    public Clickhouse(Map config) {
        super (config);
    }

    protected void prepare() {

        String format = "TabSeparated";
        if (this.config.containsKey("format")) {
            format = (String) this.config.get("format");
        }

        switch (format) {
            case "JSONEachRow":
                this.formatParse = new JSONEachRow(config);
                break;
            case "Values":
                this.formatParse = new Values(config);
                break;
            case "TabSeparated":
                this.formatParse = new TabSeparated(config);
                break;
            case "Native":
                this.formatParse = new Native(config);
                break;
            default:
                String msg = String.format("Unknown format <%s>", format);
                log.error(msg);
                System.exit(1);
                break;
        }

        this.formatParse.prepare();
        this.events = new ArrayList<>();
        if (this.config.containsKey("bulk_size")) {
            this.bulkSize = (Integer) this.config.get("bulk_size");
        } else {
            this.bulkSize = BULKSIZE;
        }
    }

    private void eventInsert(Map event) throws Exception {
        eventInsert(event, this.bulkSize);
    }

    private void eventInsert(Map event, int eventSize) throws Exception {

        this.events.add(event);

        // 重试3次
        if (this.events.size() >= eventSize + 3) {
            this.events.clear();
            log.error("Retry 3 times failed, drop this bulk, number: " + this.bulkNum);
            this.bulkNum += 1;
        }
        if (this.events.size() >= eventSize) {

            log.info("Insert bulk start, number: " + this.bulkNum);
            this.formatParse.bulkInsert(events);
            log.info("Insert bulk end, number: " + this.bulkNum);
            this.events.clear();
            this.bulkNum += 1;
        }
    }

    protected void emit(Map event) {
        try {
            eventInsert(event);
        } catch (Exception e) {
            log.error(e);
            log.warn("Insert failed");
        }
    }

    public void shutdown() {
        log.info("Start to write events to ClickHouse before shutdown");
        try {
            Thread.sleep(500);
            this.formatParse.bulkInsert(this.events);
            log.info("Succeeded to write events into ClickHouse before shutdown");
        } catch (Exception e) {
            log.error(e);
            log.info("Failed to write events into ClickHouse before shutdown");
        }
    }
}
