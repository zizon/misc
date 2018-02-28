package com.sf.misc.hive.hooks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.session.SessionState;

public class CounterHook implements ExecuteWithHookContext {

    private static final Log LOGGER = LogFactory.getLog(CounterHook.class);

    @Override
    public void run(HookContext context) throws Exception {
    }

    public static void main(String args[]){
        LOGGER.info("ok");
    }

}
