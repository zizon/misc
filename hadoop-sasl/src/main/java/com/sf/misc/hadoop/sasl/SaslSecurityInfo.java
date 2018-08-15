package com.sf.misc.hadoop.sasl;

import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SaslSecurityInfo extends SecurityInfo {
    public static final Log LOGGER = LogFactory.getLog(SaslSecurityInfo.class);

    protected static final Set<SecurityInfo> SECURITY_INFOS = Sets.newHashSet(ServiceLoader.load(SecurityInfo.class).iterator()).stream()
            .filter((info) -> !info.getClass().equals(SaslSecurityInfo.class))
            .collect(Collectors.toSet());

    @Override
    public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
        return null;
    }

    @Override
    public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
        Optional<TokenInfo> selected_token_info = SECURITY_INFOS.parallelStream()
                .map((info) -> info.getTokenInfo(protocol, conf)) //
                .filter((info) -> info != null)//
                .findFirst();

        if (selected_token_info.isPresent()) {
            LOGGER.info("using intercepted token:" + selected_token_info.get());
            return InterceptedTokenInfo.make(selected_token_info.get());
        }

        LOGGER.info("using default sasl token:" + selected_token_info.get());
        return new TokenInfo() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return null;
            }

            @Override
            public Class<? extends TokenSelector<? extends TokenIdentifier>> value() {
                return DefaultSaslTokenSelector.class;
            }
        };
    }
}
