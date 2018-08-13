package com.sf.misc.hadoop.sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

public class SaslSecurityInfo extends SecurityInfo {
    public static final Log LOGGER = LogFactory.getLog(SaslSecurityInfo.class);

    public static class SaslTokenSelector implements TokenSelector<BlockTokenIdentifier> {
        @Override
        public Token<BlockTokenIdentifier> selectToken(Text service, Collection<Token<? extends TokenIdentifier>> tokens) {
            LOGGER.info("service:" + service + " tokens:" + tokens.stream().map(Objects::toString).collect(Collectors.joining(",")));
            Token<? extends TokenIdentifier> selected = tokens.stream().filter((token) -> {
                return token.getService().equals(new Text("test service"));
            }).findAny().orElse(null);

            BlockTokenIdentifier identifier = new BlockTokenIdentifier("me", "block pool id", 123, null);
            return null;
        }
    }


    @Override
    public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
        return null;
    }

    @Override
    public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
        LOGGER.info("get token info");
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
