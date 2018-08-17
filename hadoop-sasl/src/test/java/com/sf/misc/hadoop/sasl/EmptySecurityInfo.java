package com.sf.misc.hadoop.sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;

import java.lang.annotation.Annotation;
import java.util.Collection;

public class EmptySecurityInfo extends SecurityInfo {

    public static final Log LOGGER = LogFactory.getLog(EmptySecurityInfo.class);

    public static class EmptyTokenSelector implements TokenSelector<TokenIdentifier> {

        @Override
        public Token selectToken(Text service, Collection<Token<?>> collection) {
            Token selected = collection.parallelStream().filter((token) -> {
                return token.getKind().equals(EmptyTokenIdentifier.TOKEN_KIND);
            }).findFirst().orElse(null);

            LOGGER.info("get empty token:" + selected);
            return selected;
        }
    }

    @Override
    public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
        return null;
    }

    @Override
    public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
        return new TokenInfo() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return null;
            }

            @Override
            public Class<? extends TokenSelector<? extends TokenIdentifier>> value() {
                return EmptyTokenSelector.class;
            }
        };
    }
}
