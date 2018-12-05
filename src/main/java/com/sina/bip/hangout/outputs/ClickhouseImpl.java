package com.sina.bip.hangout.outputs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;


/**
 * Created by RickyHuo on 2018/12/05.
 */


public class ClickhouseImpl<T extends List<Map>> {

    private static final Logger log = LogManager.getLogger(ClickhouseImpl.class);
    private final static int BULKSIZE = 1000;
    protected Map config;
    private int bulkNum = 0;
    private int bulkSize;
    private T listEvents;
    private FormatParse<T> formatParse;

    public ClickhouseImpl(Map config, T events) {
        this.config = config;
        this.listEvents = events;
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

        if (this.config.containsKey("flush_interval")) {
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    try {
                        formatParse.bulkInsert(listEvents);
                        log.info("Force to write to ClickHouse");
                        listEvents.clear();
                    } catch (Exception e) {
                        log.info("Failed to force to write to ClickHouse");
                        log.error(e);
                    }
                }
            };
            Timer timer = new Timer();
            long intervalPeriod = (int) this.config.get("flush_interval") * 1000;
            timer.scheduleAtFixedRate(task, 10000, intervalPeriod);
        }

        this.formatParse.prepare();
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

        this.listEvents.add(event);

        // 重试3次
        if (this.listEvents.size() >= eventSize + 3) {
            this.listEvents.clear();
            log.error("Retry 3 times failed, drop this bulk, number: " + this.bulkNum);
            this.bulkNum += 1;
        }
        if (this.listEvents.size() >= eventSize) {

            log.info("Insert bulk start, number: " + this.bulkNum);
            this.formatParse.bulkInsert(listEvents);
            log.info("Insert bulk end, number: " + this.bulkNum);
            this.listEvents.clear();
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
            this.formatParse.bulkInsert(this.listEvents);
            log.info("Succeeded to write events into ClickHouse before shutdown");
        } catch (Exception e) {
            log.error(e);
            log.info("Failed to write events into ClickHouse before shutdown");
        }
    }
}
