package com.sf.misc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import java.io.IOException;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HiveMetaSyncer {

    private static final Log LOGGER = LogFactory.getLog(HiveMetaSyncer.class);

    public static class AnyGroupMappingServiceProvider implements GroupMappingServiceProvider {
        @Override
        public List<String> getGroups(String user) throws IOException {
            return Collections.emptyList();
        }

        @Override
        public void cacheGroupsRefresh() throws IOException {
        }

        @Override
        public void cacheGroupsAdd(List<String> groups) throws IOException {
        }
    }

    public static class MetaPack {
        private Table table;
        private Database database;

        public Database getDatabase() {
            return database;
        }

        public void setDatabase(Database database) {
            this.database = database;
        }

        private Collection<Partition> partitions;

        public Table getTable() {
            return table;
        }

        public void setTable(Table table) {
            this.table = table;
        }

        public Collection<Partition> getPartitions() {
            return partitions;
        }

        public void setPartitions(Collection<Partition> partitions) {
            this.partitions = partitions;
        }
    }

    protected ConcurrentMap<String, IMetaStoreClient> clients = new ConcurrentHashMap<>();

    /**
     * collect metastore infos
     *
     * @param meta_uri metastore uri,such as thrift://${host}:${port}
     * @return the collected meta datas
     * @throws HiveException when fetching from metastore fails
     */
    public Iterable<MetaPack> collect(String meta_uri) throws HiveException {
        return new Iterable<MetaPack>() {
            private IMetaStoreClient hive = HiveMetaSyncer.this.connect(meta_uri);

            @Override
            public Iterator<MetaPack> iterator() {
                return new Iterator<MetaPack>() {
                    private String current_database;
                    private String current_table;
                    private Queue<String> databases;
                    private Queue<String> tables;

                    @Override
                    public boolean hasNext() {
                        // ensure current database
                        if (this.current_database == null) {
                            // ensure databases
                            if (this.databases == null) {
                                try {
                                    this.databases = new LinkedList<>(hive.getAllDatabases());
                                } catch (TException e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            // more database to process?
                            String head = this.databases.poll();
                            if (head == null) {
                                return false;
                            }

                            // update current
                            this.current_database = head;
                            this.current_table = null;
                        }

                        if (this.current_table != null) {
                            return true;
                        }

                        // ensure current table
                        if (this.tables == null) {
                            try {
                                this.tables = new LinkedList<>(hive.getAllTables(this.current_database));
                            } catch (TException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        // more tables to process?
                        String head = this.tables.poll();
                        if (head != null) {
                            // update table
                            this.current_table = head;
                            return true;
                        }

                        // consume all table under curretn_database
                        // try next
                        this.current_database = null;
                        this.current_table = null;
                        this.tables = null;
                        return this.hasNext();
                    }

                    @Override
                    public MetaPack next() {
                        try {
                            MetaPack pack = new MetaPack();
                            pack.setTable(hive.getTable(this.current_database, this.current_table));
                            pack.setPartitions(hive.listPartitions(this.current_database, this.current_table, Short.MAX_VALUE));
                            pack.setDatabase(hive.getDatabase(pack.getTable().getDbName()));

                            // remove location info
                            pack.getDatabase().setLocationUriIsSet(false);
                            pack.getTable().getSd().setLocationIsSet(false);
                            pack.getPartitions().forEach(partition ->
                                    partition.getSd().setLocationIsSet(false)
                            );

                            // consume this table
                            this.current_table = null;

                            return pack;
                        } catch (TException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
            }
        };
    }

    /**
     * sync metastore specified by from_uri to to_uri
     *
     * @param to_uri   the target metastore
     * @param from_url the source metastore
     * @throws HiveException throws when performing metastore operation fails
     */
    public void syncTo(String to_uri, String from_url) throws HiveException {
        IMetaStoreClient to = this.connect(to_uri);

        // bookeeping for database creation
        Set<String> exists_database = new TreeSet<>();
        try {
            exists_database.addAll(to.getAllDatabases());
        } catch (TException e) {
            throw new HiveException("fail to get database for uri:" + to_uri, e);
        }

        for (MetaPack meta : this.collect(from_url)) {
            String table_name = meta.getTable().getDbName() + "." + meta.getTable().getTableName();
            LOGGER.info("process table:" + table_name);

            Table table = meta.getTable();
            try {
                // ensure database
                if (!exists_database.contains(table.getDbName())) {
                    LOGGER.info("create database:" + meta.getDatabase());
                    try {
                        to.createDatabase(meta.getDatabase());
                    } catch (AlreadyExistsException e) {
                        // in some how,others may have create this database
                        // gapping us from knowing that.
                        // so just catch it
                    } finally {
                        // do not forget to add back
                        exists_database.add(table.getDbName());
                    }
                }

                // ensure tables
                if (to.tableExists(table.getDbName(), table.getTableName())) {
                    LOGGER.info("alter table:" + table_name);

                    // fix table location
                    // as they may not resist in a same filesystem
                    String location = to.getTable(table.getDbName(), table.getTableName()).getSd().getLocation();
                    table.getSd().setLocation(location);

                    // fix partition location
                    meta.getPartitions().forEach((partition) ->
                            partition.getSd().setLocation(location)
                    );

                    // do alter
                    to.alter_table(table.getDbName(), table.getTableName(), table);
                } else {
                    LOGGER.info("create table:" + table);
                    to.createTable(table);
                }

                // patch partitions
                if (meta.getPartitions().size() > 0) {
                    LOGGER.info("add partitions for table:" + table_name);
                    to.add_partitions(new LinkedList<>(meta.getPartitions()), true, false);
                }
            } catch (TException e) {
                throw new HiveException("alter table:" + table + " fail", e);
            }
        }
    }

    protected IMetaStoreClient newClient(String meta_uri) throws HiveException {
        HiveConf configuration = new HiveConf();

        // guide meta store location
        configuration.set("hive.metastore.uris", meta_uri);

        // overwrite ugi.
        // workaround for windows
        configuration.setClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING, AnyGroupMappingServiceProvider.class, GroupMappingServiceProvider.class);
        UserGroupInformation.setConfiguration(configuration);

        try {
            return new HiveMetaStoreClient(configuration);
        } catch (MetaException e) {
            throw new HiveException(e);
        }
    }

    protected IMetaStoreClient connect(String meta_uri) throws HiveException {
        try {
            return this.clients.compute(meta_uri, (uri, old) -> {
                // create if missing
                if (old == null) {
                    try {
                        return newClient(uri);
                    } catch (HiveException exception) {
                        throw new RuntimeException("fail to create meta client for uri:" + uri, exception);
                    }
                }

                return old;
            });
        } catch (Exception exception) {
            throw new HiveException(exception);
        }
    }

    public static void main(String[] args) {
        try {
            final String from_url = "thrift://10.202.34.209:9083";
            final String to_url = "thrift://10.202.77.200:9083";

            new HiveMetaSyncer().syncTo(to_url, from_url);
        } catch (Exception e) {
            LOGGER.error("unexpected  exception", e);
        } finally {
            LOGGER.info("done");
        }
    }
}
