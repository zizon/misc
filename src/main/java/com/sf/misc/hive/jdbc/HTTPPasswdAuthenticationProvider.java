package com.sf.misc.hive.jdbc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * to activate this for hive jdbc server,
 * add configuration bellow:
 * hive.server2.authentication = CUSTOM
 * hive.server2.custom.authentication.class = com.sf.misc.hive.jdbc.HTTPPasswdAuthenticationProvider
 */
public class HTTPPasswdAuthenticationProvider implements PasswdAuthenticationProvider {

    private static final Log LOGGER = LogFactory.getLog(HTTPPasswdAuthenticationProvider.class);

    public static final String AUTH_SERVER_URL = "sf.com.http.auth.url";

    public static final ScheduledExecutorService POOL = Executors.newScheduledThreadPool(1);
    protected static ConcurrentLinkedQueue<WeakReference<HTTPPasswdAuthenticationProvider>> CLEANER = new ConcurrentLinkedQueue<>();

    {
        POOL.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                Iterator<WeakReference<HTTPPasswdAuthenticationProvider>> iterator = CLEANER.iterator();
                while (iterator.hasNext()) {
                    WeakReference<HTTPPasswdAuthenticationProvider> current = iterator.next();
                    HTTPPasswdAuthenticationProvider provider = current.get();

                    if (provider == null) {
                        // collected
                        iterator.remove();
                        continue;
                    }

                    // now evict
                    Iterator<Map.Entry<String, Map.Entry<String, Long>>> cache_entrys = provider.cacheIterator();
                    while (cache_entrys.hasNext()) {
                        Map.Entry<String, Map.Entry<String, Long>> entry = cache_entrys.next();

                        // find access info
                        Map.Entry<String, Long> access_info = entry.getValue();
                        long then = access_info.getValue();
                        if (now - then >= 1000 * 60) {
                            cache_entrys.remove();
                            LOGGER.info("expire user:" + entry.getKey());
                        }
                    }
                }
            }
        }, 0, 1, TimeUnit.MINUTES);
    }


    protected Configuration conf;
    protected ConcurrentMap<String, Map.Entry<String, Long>> caches;
    protected String auth_url;

    public HTTPPasswdAuthenticationProvider(HiveConf conf) {
        this(conf, true);
    }

    public HTTPPasswdAuthenticationProvider(Configuration conf, boolean ignore) {
        super();

        // do a copy
        this.conf = new Configuration(conf);

        this.auth_url = this.conf.get(AUTH_SERVER_URL);
        if (this.auth_url == null) {
            throw new NullPointerException("no valid " + AUTH_SERVER_URL);
        }

        // setup cache
        this.caches = new ConcurrentHashMap<>();

        // register
        CLEANER.offer(new WeakReference<>(this));
    }

    @Override
    public void Authenticate(final String user, final String password) throws AuthenticationException {

        // sanity check
        if (user == null) {
            throw new AuthenticationException("no user for authentication");
        } else if (password == null) {
            throw new AuthenticationException("no password provided for authentication ,of user:" + user);
        }


        // find authentication cache
        Map.Entry<String, Long> hit = this.caches.get(user);
        if (hit == null) {
            try {
                Future<Boolean> ok = this.asyncValidate(user, password);
                // a bit tricky,try if other had already kick cache up
                hit = this.caches.get(user);
                if (hit == null) {
                    if (ok.get()) {
                        hit = new Map.Entry<String, Long>() {
                            private Long access_time = System.currentTimeMillis();
                            private String saved_password = password;

                            @Override
                            public String getKey() {
                                return this.saved_password;
                            }

                            @Override
                            public Long getValue() {
                                return access_time;
                            }

                            @Override
                            public Long setValue(Long value) {
                                Long old = this.access_time;
                                this.access_time = value;
                                return old;
                            }
                        };

                        // cache it
                        this.caches.put(user, hit);
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.warn("interrupt when validating user:" + user, e);
            } catch (ExecutionException e) {
                LOGGER.error("unexpected execption when validating user:" + user, e);
            }
        }

        // no password info for user
        if (hit == null) {
            throw new AuthenticationException("password not match of user:" + user);
        }

        // renew cache
        hit.setValue(System.currentTimeMillis());

        // check
        if (password.compareTo(hit.getKey()) != 0) {
            throw new AuthenticationException("password not match of user:" + user);
        }
    }

    protected ExecutorService pool() {
        return POOL;
    }

    protected Future<Boolean> asyncValidate(final String user, final String password) {
        return this.pool().submit(() -> {
            return this.validate(user, password);
        });
    }

    protected boolean validate(String user, String password) {
        boolean ok = false;
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL("http://" + auth_url + "/j_spring_security_check").openConnection();
            connection.setDoOutput(true);
            connection.getOutputStream().write(("j_username=" + user + "&" + "j_password=" + password).getBytes());
            connection.connect();
            if (connection.getResponseCode() == 200) {
                LOGGER.info("validate user ok:" + user);
                ok = true;
            } else {
                LOGGER.warn("fail to validate user:" + user + " http:" + connection.getResponseCode() + " message:" + connection.getResponseMessage());
            }
        } catch (IOException e) {
            LOGGER.error("fail to validate user", e);
            ok = false;
        }

        return ok;
    }

    protected Iterator<Map.Entry<String, Map.Entry<String, Long>>> cacheIterator() {
        return this.caches.entrySet().iterator();
    }

    public static void main(String[] args) {
        LOGGER.info("start");
        try {
            Configuration configuration = new Configuration();
            configuration.set(AUTH_SERVER_URL, "10.202.77.200:6080");
            HTTPPasswdAuthenticationProvider provider = new HTTPPasswdAuthenticationProvider(configuration, true);
            provider.Authenticate("hive", "hive1");
        } catch (AuthenticationException e) {
            LOGGER.error("unexpected failure", e);
        }
    }

}
