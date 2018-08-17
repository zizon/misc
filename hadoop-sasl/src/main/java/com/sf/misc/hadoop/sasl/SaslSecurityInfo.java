package com.sf.misc.hadoop.sasl;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;

import java.lang.annotation.Annotation;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SaslSecurityInfo extends SecurityInfo {
    public static final Log LOGGER = LogFactory.getLog(SaslSecurityInfo.class);

    public static final String STRICT_CHECK = "com.sf.hadoop.security.auth.strict_check";

    protected static final Set<SecurityInfo> SECURITY_INFOS = Sets.newHashSet(ServiceLoader.load(SecurityInfo.class).iterator()).stream()
            .filter((info) -> !info.getClass().equals(SaslSecurityInfo.class))
            .collect(Collectors.toSet());

    protected static final Configuration REALOADABLE_CONFIG = new Configuration();
    protected static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("refresh-security-info-thread-%d").build());

    static {
        SCHEDULER.scheduleAtFixedRate(() -> {
            REALOADABLE_CONFIG.reloadConfiguration();
        }, 0, 1, TimeUnit.MINUTES);
    }

    public static boolean strictCheck() {
        return REALOADABLE_CONFIG.getBoolean(STRICT_CHECK, false);
    }

    @Override
    public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
        return null;
    }

    @Override
    public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
        // find proxied tokeninfo
        Optional<TokenInfo> selected_token_info = SECURITY_INFOS.parallelStream()
                .map((info) -> info.getTokenInfo(protocol, conf)) //
                .filter((info) -> info != null)//
                .findFirst();

        // has token,do intercept
        if (selected_token_info.isPresent()) {
            LOGGER.info("using intercepted token selector:" + selected_token_info.get());
            return InterceptedTokenInfo.make(selected_token_info.get());
        }

        LOGGER.info("using default sasl token selector");
        return new TokenInfo() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return null;
            }

            @Override
            public Class<? extends TokenSelector<? extends TokenIdentifier>> value() {
                return SaslTokenSelector.class;
            }
        };
    }
}
