package com.sina.bip.hangout.outputs;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import com.ctrip.ops.sysdev.baseplugin.BaseOutput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by RickyHuo on 2017/09/23.
 */


public class Clickhouse extends BaseOutput {

    private static final Logger log = LogManager.getLogger(Clickhouse.class);
    private ClickhouseImpl impl;

    public Clickhouse(Map config) {
        super (config);
    }

    protected void prepare() {

        if (!this.config.containsKey("flush_interval")) {
            ArrayList events = new ArrayList<Map>();
            impl = new ClickhouseImpl<ArrayList<Map>>(this.config, events);
        } else {
            CopyOnWriteArrayList events = new CopyOnWriteArrayList<Map>();
            impl = new ClickhouseImpl<CopyOnWriteArrayList<Map>>(this.config, events);
        }

        impl.prepare();
    }


    protected void emit(Map event) {
        impl.emit(event);
    }

    public void shutdown() {
        impl.shutdown();
    }
}
