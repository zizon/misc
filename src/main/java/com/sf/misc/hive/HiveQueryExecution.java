package com.sf.misc.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.jdbc.HiveStatement;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

public class HiveQueryExecution {

    private static final Log LOGGER = LogFactory.getLog(HiveQueryExecution.class);

    public static final ExecutorService COMMON_POOL = new ForkJoinPool();

    protected void closeStatment(HiveStatement statement) {
        try {
            statement.close();
        } catch (SQLException exception) {
            LOGGER.error("fail when close statment", exception);
        }
    }

    public void execute(HiveStatement statement, String query, boolean cleanup) {
        try {
            // submit query
            statement.executeAsync(query);
        } catch (SQLException exception) {
            LOGGER.error("fail to preocess query:" + query, exception);
            // let caller to do cleanup?
            if (cleanup) {
                this.closeStatment(statement);
            }
            return;
        }

        AtomicBoolean done = new AtomicBoolean(false);
        // fetch logs
        COMMON_POOL.execute(new Runnable() {
            @Override
            public void run() {
                boolean greedy_cehck = false;
                do {
                    try {
                        for (String log : statement.getQueryLog(true, 10)) {
                            //TODO do write
                        }
                    } catch (SQLException exception) {
                        LOGGER.error("fail to get query log for:" + query, exception);
                    }

                    if (greedy_cehck) {
                        // finished greedy check,
                        // query is done,and query log may have all fetched, hopefully.
                        // safe to set back to false
                        greedy_cehck = false;
                        break;
                    }

                    // quit if statement is closed
                    if (!done.get()) {
                        // queue self again
                        COMMON_POOL.execute(this);
                        return;
                    }

                    // notified query is complete,
                    // but not sure if complete log is fetched,
                    // so do it again
                    greedy_cehck = true;
                } while (greedy_cehck);
            }
        });

        try {
            statement.getUpdateCount();
        } catch (SQLException exception) {
            LOGGER.error("unexpected exception when waiting for query complection:" + query, exception);
            done.set(true);
            if (cleanup) {
                this.closeStatment(statement);
            }
            return;
        }

        // notify log poller
        done.set(true);

        // here,either query is complete
        // or aborted.
        //TODO generate id and write back result etc...
        return;
    }

}
