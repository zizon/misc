package com.sf.misc.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveMetaStore {

    @Test
    public void test() throws Exception {
        HiveConf conf = new HiveConf();
        conf.set("fs.defaultFS", "hdfs://test-cluster");
        conf.set("javax.jdo.option.ConnectionURL", "jdbc:mysql://test:3306/metastore?dontTrackOpenResources=true&amp;createDatabaseIfNotExist=true");
        conf.set("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver");
        conf.set("javax.jdo.option.ConnectionUserName", "hive");
        conf.set("javax.jdo.option.ConnectionPassword", "hive");
        conf.set("datanucleus.cache.queryResults.type", "none");
        conf.set("dfs.nameservices", "test-cluster");
        conf.set("dfs.ha.namenodes.test-cluster", "namenode1,namenode2");
        conf.set("dfs.namenode.rpc-address.test-cluster.namenode1", "10.202.77.200:8020");
        conf.set("dfs.namenode.rpc-address.test-cluster.namenode2", "10.202.77.201:8020");
        conf.set("dfs.client.failover.proxy.provider.test-cluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
/*
        IHMSHandler handler = new HiveMetaStore.HMSHandler("test", conf, true);
        Assert.assertNotNull(handler);
        Assert.assertTrue(handler.get_table("default", "test") != null);
*/
        conf.setBoolean("hive.optimize.skewjoin", true);
        conf.setBoolean("hive.groupby.skewindata", true);
        /*
        SessionState session = new SessionState(conf);
        SessionState.setCurrentSessionState(session);
        //SessionState.start(new SessionState(conf));

        String command = "desc extended stg_test_xiaobin.test_with_partition partition(inc_day);";
        Driver driver = new Driver(conf);
        driver.run(command);
        */
        String table = "test_with_partition";
        System.out.println(table.split("\\.").length);
    }
}
