package com.sf.misc.hive.jdbc;

import com.sf.misc.ReferenceBaseCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
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

    protected Configuration conf;
    protected ReferenceBaseCache<String> caches;
    protected String auth_url;
    protected boolean enable;

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

        // setup cache
        this.caches = new ReferenceBaseCache<>("http-user-password-authentication-cache");

        // scheduler reload
        ReferenceBaseCache.pool().scheduleAtFixedRate(() -> {
            reloadablConfiguration();
        }, 0, 1, TimeUnit.MINUTES);
    }

    @Override
    public void Authenticate(final String user, final String password) throws AuthenticationException {
        String saved_password = this.caches.fetch(user, (user_name) -> {
            if (this.validate(user_name, password)) {
                return password;
            }
            return null;
        });

        if (saved_password == null || password.compareTo(saved_password) != 0) {
            LOGGER.warn("auth user:" + user + " fail, enabled:" + this.enable);
            if (this.enable) {
                this.failAuthenticate("password not match of user:" + user);
            }
            return;
        }

        LOGGER.info("auth user:" + user + " ok");
        return;
    }

    protected void reloadablConfiguration() {
        LOGGER.info("reload configuration");
        this.conf.reloadConfiguration();

        this.auth_url = this.conf.get(AUTH_SERVER_URL);
        if (this.auth_url == null) {
            throw new NullPointerException("no valid " + AUTH_SERVER_URL);
        }

        this.enable = this.conf.getBoolean(AUTH_ENABLE, false);
    }

    protected void failAuthenticate(String reason) throws AuthenticationException {
        throw new AuthenticationException(reason);
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

    public static void main(String[] args) {
        LOGGER.info("start");

        Configuration configuration = new Configuration();
        configuration.set(AUTH_SERVER_URL, "10.202.77.200:6080");
        HTTPPasswdAuthenticationProvider provider = new HTTPPasswdAuthenticationProvider(configuration, true);
        for (int i = 0; i < 100; i++) {
            try {
                provider.Authenticate("hive", "hive1");
                LOGGER.info("auth ok");
            } catch (AuthenticationException e) {
                LOGGER.error("unexpected failure", e);
            }
        }

        LockSupport.park();
    }

}
