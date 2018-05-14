package com.sf.misc.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.authorization.hive.authorizer.RangerHiveAccessRequest;
import org.apache.ranger.authorization.hive.authorizer.RangerHiveResource;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.service.RangerBasePlugin;
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

    static class RangerHivePlugin extends RangerBasePlugin {
        public static boolean UpdateXaPoliciesOnGrantRevoke = RangerHadoopConstants.HIVE_UPDATE_RANGER_POLICIES_ON_GRANT_REVOKE_DEFAULT_VALUE;
        public static boolean BlockUpdateIfRowfilterColumnMaskSpecified = RangerHadoopConstants.HIVE_BLOCK_UPDATE_IF_ROWFILTER_COLUMNMASK_SPECIFIED_DEFAULT_VALUE;
        public static String DescribeShowTableAuth = RangerHadoopConstants.HIVE_DESCRIBE_TABLE_SHOW_COLUMNS_AUTH_OPTION_PROP_DEFAULT_VALUE;

        private static String RANGER_PLUGIN_HIVE_ULRAUTH_FILESYSTEM_SCHEMES = "ranger.plugin.hive.urlauth.filesystem.schemes";
        private static String RANGER_PLUGIN_HIVE_ULRAUTH_FILESYSTEM_SCHEMES_DEFAULT = "hdfs:,file:";
        private static String FILESYSTEM_SCHEMES_SEPARATOR_CHAR = ",";
        private String[] fsScheme = null;

        public RangerHivePlugin(String appType) {
            super("hive", appType);
        }

        @Override
        public void init() {
            super.init();

            RangerConfiguration.getInstance().set("ranger.plugin.hive.policy.rest.url", "http://10.202.77.200:6088");

            RangerHivePlugin.UpdateXaPoliciesOnGrantRevoke = RangerConfiguration.getInstance().getBoolean(RangerHadoopConstants.HIVE_UPDATE_RANGER_POLICIES_ON_GRANT_REVOKE_PROP, RangerHadoopConstants.HIVE_UPDATE_RANGER_POLICIES_ON_GRANT_REVOKE_DEFAULT_VALUE);
            RangerHivePlugin.BlockUpdateIfRowfilterColumnMaskSpecified = RangerConfiguration.getInstance().getBoolean(RangerHadoopConstants.HIVE_BLOCK_UPDATE_IF_ROWFILTER_COLUMNMASK_SPECIFIED_PROP, RangerHadoopConstants.HIVE_BLOCK_UPDATE_IF_ROWFILTER_COLUMNMASK_SPECIFIED_DEFAULT_VALUE);
            RangerHivePlugin.DescribeShowTableAuth = RangerConfiguration.getInstance().get(RangerHadoopConstants.HIVE_DESCRIBE_TABLE_SHOW_COLUMNS_AUTH_OPTION_PROP, RangerHadoopConstants.HIVE_DESCRIBE_TABLE_SHOW_COLUMNS_AUTH_OPTION_PROP_DEFAULT_VALUE);

            String fsSchemesString = RangerConfiguration.getInstance().get(RANGER_PLUGIN_HIVE_ULRAUTH_FILESYSTEM_SCHEMES, RANGER_PLUGIN_HIVE_ULRAUTH_FILESYSTEM_SCHEMES_DEFAULT);
            fsScheme = StringUtils.split(fsSchemesString, FILESYSTEM_SCHEMES_SEPARATOR_CHAR);

            if (fsScheme != null) {
                for (int i = 0; i < fsScheme.length; i++) {
                    fsScheme[i] = fsScheme[i].trim();
                }
            }
        }

        public String[] getFSScheme() {
            return fsScheme;
        }
    }
}
