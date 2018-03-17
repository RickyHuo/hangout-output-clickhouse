package com.sina.bip.hangout.outputs;

import java.util.List;
import java.util.Map;

public interface FormatParse {

    public void bulkInsert(List<Map> events) throws Exception;
    public void prepare();
}
