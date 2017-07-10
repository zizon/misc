package com.sf.misc;

import org.apache.calcite.rel.core.Collect;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.security.GroupMappingServiceProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.codehaus.janino.util.Producer;

import java.io.IOException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
        Set<String> exists_database = new ConcurrentSkipListSet<>();
        try {
            exists_database.addAll(to.getAllDatabases());
        } catch (TException e) {
            throw new HiveException("fail to get database for uri:" + to_uri, e);
        }

        // concurrent sync
        ExecutorService executors = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        Queue<IMetaStoreClient> pool = new ConcurrentLinkedQueue<>();
        for (MetaPack meta : this.collect(from_url)) {
            executors.execute(() -> {
                try {
                    syncTable(pool, exists_database, meta, () -> {
                        try {
                            return newClient(to_uri);
                        } catch (HiveException e) {
                            throw new RuntimeException(e);
                        }
                    });
                } catch (HiveException e) {
                    LOGGER.error("fail to process table:" + meta.getTable(), e);
                }
            });
        }

        // wait for all
        try {
            LOGGER.info("wait for sync finished");
            executors.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            LOGGER.warn("operation interrupted, some may not take effect");
        } finally {
            pool.parallelStream().forEach((client) -> {
                client.close();
            });
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

    protected void syncTable(Queue<IMetaStoreClient> pool, Set<String> exists_database, MetaPack meta, Producer<IMetaStoreClient> pool_producer) throws HiveException {
        // prepare client
        IMetaStoreClient to = pool.poll();
        if (to == null) {
            try {
                to = pool_producer.produce();
            } catch (Exception e) {
                throw new HiveException("fail to get a client", e);
            }
        }

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

                StringBuilder buffer = new StringBuilder();
                buffer.setLength(0);

                // collect partitions that on 'to'
                Set<String> to_partitions = new HashSet<>();
                for (Partition partition : to.listPartitions(table.getDbName(), table.getTableName(), Short.MAX_VALUE)) {
                    buffer.setLength(0);
                    for (String value : partition.getValues()) {
                        buffer.append(value);
                    }

                    to_partitions.add(buffer.toString());
                }

                // filter that already exists
                List<Partition> new_partitions = meta.getPartitions().stream().filter(partition -> {
                    buffer.setLength(0);
                    for (String value : partition.getValues()) {
                        buffer.append(value);
                    }
                    return !to_partitions.contains(buffer.toString());
                }).collect(Collectors.toList());

                // increment update
                if (new_partitions.size() > 0) {
                    LOGGER.info("add partitions for table: " + table_name + " count:" + new_partitions.size());
                    to.add_partitions(new_partitions, true, false);
                } else {
                    LOGGER.info("partitions up to date:" + table_name);
                }
            }
        } catch (TException e) {
            throw new HiveException("alter table:" + table + " fail", e);
        } finally {
            if (to != null) {
                pool.add(to);
            }
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
