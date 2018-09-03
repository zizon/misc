package com.sf.misc.hive.hooks;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Set;
import java.util.stream.Collectors;

public class CounterHook implements PreExecute {

    private static final Log LOGGER = LogFactory.getLog(CounterHook.class);

    @Override
    public void run(SessionState sess, Set<ReadEntity> inputs, Set<WriteEntity> outputs, UserGroupInformation ugi) throws Exception {
        System.out.println("input read entity:" + inputs.stream().map((entity) -> {
            StringBuilder builder = new StringBuilder();
            builder.append(entity.toString());
            return builder.toString();
        }).collect(Collectors.joining(",\n"))  //
                + "\noutput write entity:" + outputs.stream().map((entity) -> {
            StringBuilder builder = new StringBuilder();
            builder.append(entity.toString());
            return builder.toString();
        }).collect(Collectors.joining(",\n")));
    }
}
