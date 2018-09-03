package com.sf.misc.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;


public class TestParser {

    @Test
    public void test() throws Exception {
        String query = " select waybill_no,\n" +
                "       asu_dept_code,\n" +
                "       rn,\n" +
                "       lead(asu_dept_code, 1) OVER(PARTITION BY waybill_no ORDER BY rn) asu_dept_code_next" +
                "  from abm_fct_dist_waybill_tmp021\n" +
                " where waybill_no = '012231645662'\n" +
                " order by rn";

        String meta_uri = "thrift://10.202.77.200:9083";
        HiveConf configuration = new HiveConf();
        // guide meta store location
        configuration.set("fs.defaultFS", "hdfs://test-cluster");

        String hosts = "10.202.77.200:8020,10.202.77.201:8020";
        String nameservice = "test-cluster";
        configuration.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + nameservice);
        System.out.println(configuration.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));

        // nameservice ha provider
        configuration.set(DFSConfigKeys.DFS_NAMESERVICES, nameservice);
        configuration.setClass(DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX + "." + nameservice, ConfiguredFailoverProxyProvider.class, FailoverProxyProvider.class);

        // set namenodes
        configuration.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + nameservice, Arrays.stream(hosts.split(",")).map((host) -> {
            String namenode_id = host;
            configuration.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nameservice + "." + namenode_id, host);
            return namenode_id;
        }).collect(Collectors.joining(",")));

        configuration.set("hive.metastore.uris", meta_uri);
        //configuration.set("hive.exec.scratchdir","hdfs://test-cluster/tmp/hive");
        //SessionState.start(configuration);

        configuration.set("hive.session.id", "883b0899-b0dc-4d9e-8517-c07fd66b5ffb");
        //configuration.set("hive.exec.local.scratchdir", "d:\\user\\01369351\\桌面\\misc\\hive-query-planer");
        // overwrite ugi.
        // workaround for windows
        //configuration.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING, HiveMetaSyncer.AnyGroupMappingServiceProvider.class, GroupMappingServiceProvider.class);
        //UserGroupInformation.setConfiguration(configuration);
        //Context context = new Context(configuration);
        //ParseDriver driver = new ParseDriver();
        //ASTNode ast = driver.parse(query);

        SessionState.start(configuration);
        //System.out.println(ast);
        Driver driver = new Driver(configuration);
        driver.init();
        driver.run("select * from test limit 10");
    }
}
