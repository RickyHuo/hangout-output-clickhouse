package com.sina.bip.hangout.outputs;

import java.util.List;
import java.util.Map;

public interface FormatParse<T extends List<Map>> {

    public void bulkInsert(T events) throws Exception;
    public void prepare();
}
