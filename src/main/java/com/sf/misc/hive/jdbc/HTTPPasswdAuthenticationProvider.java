package com.sf.misc.hive.jdbc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * to activate this for hive jdbc server,
 * add configuration bellow:
 * hive.server2.authentication = CUSTOM
 * hive.server2.custom.authentication.class = com.sf.misc.hive.jdbc.HTTPPasswdAuthenticationProvider
 */
public class HTTPPasswdAuthenticationProvider implements PasswdAuthenticationProvider {

    private static final Log LOGGER = LogFactory.getLog(HTTPPasswdAuthenticationProvider.class);

    public static final String AUTH_SERVER_URL = "sf.com.http.auth.url";
    public static final String AUTH_ENABLE = "sf.com.http.auth.enable";
    public static final String AUTH_TIMEOUT = "sf.com.http.auth.timeout";
    public static final String AUTH_USE_CACHE = "sf.com.http.auth.usecache";

    public static final ScheduledExecutorService POOL = Executors.newScheduledThreadPool(1);

    protected static ConcurrentLinkedQueue<SoftReference<HTTPPasswdAuthenticationProvider>> REFRESH_QUEUE = new ConcurrentLinkedQueue<>();
    protected static ConcurrentMap<String, String> AUTH_CACHE = new ConcurrentHashMap<>();

    static {
        // scheduler reload
        POOL.scheduleAtFixedRate(() -> {
            Iterator<SoftReference<HTTPPasswdAuthenticationProvider>> iterator = REFRESH_QUEUE.iterator();
            while (iterator.hasNext()) {
                SoftReference<HTTPPasswdAuthenticationProvider> holder = iterator.next();
                HTTPPasswdAuthenticationProvider provider = holder.get();

                // collected
                if (provider == null) {
                    iterator.remove();
                    continue;
                }

                // refresh
                provider.reloadablConfiguration();
            }

            // evict auth cache
            Iterator<?> auth_cache_iterator = AUTH_CACHE.entrySet().iterator();

            // keep first 1000 entry
            int countdown = 1000;
            while (auth_cache_iterator.hasNext()) {
                iterator.next();
                if (countdown-- <= 0) {
                    iterator.remove();
                }
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    protected Configuration conf;

    @SuppressWarnings("unused")
    public HTTPPasswdAuthenticationProvider(HiveConf conf) {
        this(conf, true);
    }

    @SuppressWarnings("unused")
    public HTTPPasswdAuthenticationProvider(Configuration conf, boolean ignore) {
        super();

        // do a copy
        this.conf = new Configuration(conf);

        // kick reloadable configuration
        this.reloadablConfiguration();

        // scheduler reload
        REFRESH_QUEUE.offer(new SoftReference<>(this));
    }

    @Override
    public void Authenticate(final String user, final String password) throws AuthenticationException {
        boolean enable = conf.getBoolean(AUTH_ENABLE, false);
        if (enable) {
            this.authenticate(user, password);
            return;
        } else {
            LOGGER.info("auth disabled, let user:" + user + " go");
            POOL.submit(() -> {
                try {
                    authenticate(user, password);
                } catch (AuthenticationException exception) {
                    LOGGER.warn("log fail auth for user:" + user, exception);
                }
            });
            return;
        }

    }

    public void authenticate(final String user, final String password) throws AuthenticationException {
        String saved_password = null;

        // use cache?
        if (this.conf.getBoolean(AUTH_USE_CACHE, true)) {
            saved_password = AUTH_CACHE.get(user);
        }

        // validate password
        if (saved_password == null) {
            // no saved password
            // validate now
            if (this.validate(user, password)) {
                // validate ok
                saved_password = password;
                AUTH_CACHE.put(user, password);

                LOGGER.info("validate user:" + user + " ok");
                return;
            } else {
                this.failAuthenticate("validate user fail,may be timeout");
                return;
            }
        }

        if (password == null) {
            this.failAuthenticate("user:" + user + " provide no password");
            return;
        } else if (saved_password.compareTo(password) != 0) {
            this.failAuthenticate("password not match of user:" + user);
            return;
        }

        // password match
        AUTH_CACHE.put(user, password);
        LOGGER.info("validate user:" + user + " ok");
    }

    protected void reloadablConfiguration() {
        LOGGER.info("reload configuration");
        this.conf.reloadConfiguration();
    }

    protected void failAuthenticate(String reason) throws AuthenticationException {
        throw new AuthenticationException(reason);
    }

    protected boolean validate(String user, String password) {
        boolean ok = false;
        String auth_url = conf.get(AUTH_SERVER_URL);
        if (auth_url == null) {
            LOGGER.warn(AUTH_SERVER_URL + " is null, setting " + AUTH_ENABLE + " to true will block all user");
            return false;
        }

        int timeout = this.conf.getInt(AUTH_TIMEOUT, 5000);
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL("http://" + auth_url + "/j_spring_security_check").openConnection();

            // add timeout
            connection.setConnectTimeout(timeout);
            connection.setReadTimeout(timeout);

            connection.setDoOutput(true);
            connection.getOutputStream().write(("j_username=" + user + "&" + "j_password=" + password).getBytes());
            connection.connect();
            if (connection.getResponseCode() == 200) {
                LOGGER.info("validate user ok:" + user);
                ok = true;
            } else {
                ok = false;
                LOGGER.warn("fail to validate user:" + user + " http:" + connection.getResponseCode() + " message:" + connection.getResponseMessage());
            }
        } catch (IOException e) {
            LOGGER.error("fail to validate user:" + user, e);
            ok = false;
        } finally {
            if (connection != null) {
                try {
                    connection.disconnect();
                } catch (Exception exception) {
                    LOGGER.warn("unexpected exception when closing auth connection", exception);
                }
            }
        }

        return ok;
    }

    public static void main(String[] args) {
        LOGGER.info("start");

        Configuration configuration = new Configuration();
        configuration.setBoolean(AUTH_ENABLE, true);
        configuration.setBoolean(AUTH_USE_CACHE, true);
        configuration.set(AUTH_SERVER_URL, "10.202.77.200:6080");
        HTTPPasswdAuthenticationProvider provider = new HTTPPasswdAuthenticationProvider(configuration, true);
        for (int i = 0; i < 100; i++) {
            try {
                provider.Authenticate("hive", "hive1");
            } catch (AuthenticationException e) {
                LOGGER.error("unexpected failure", e);
            }
        }

        LockSupport.park();
    }

}
