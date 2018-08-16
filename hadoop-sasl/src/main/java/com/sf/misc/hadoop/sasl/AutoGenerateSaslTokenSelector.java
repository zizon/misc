package com.sf.misc.hadoop.sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class AutoGenerateSaslTokenSelector implements TokenSelector<SaslTokenIdentifier> {

    public static final Log LOGGER = LogFactory.getLog(AutoGenerateSaslTokenSelector.class);

    protected Optional<Token<? extends TokenIdentifier>> saslToken(Text service, Collection<Token<? extends TokenIdentifier>> tokens) {
        return tokens.parallelStream()
                .filter((token) -> token.getKind().equals(SaslTokenIdentifier.TOKEN_KIND))
                .filter((token) -> {
                    try {
                        SaslTokenIdentifier identifier = new SaslTokenIdentifier();
                        identifier.readFields(new DataInputStream(new ByteArrayInputStream(token.getIdentifier())));
                        return identifier.integrityCheck();
                    } catch (IOException e) {
                        throw new UncheckedIOException("fail to read token", e);
                    }
                })
                .findAny();
    }

    @Override
    public Token<SaslTokenIdentifier> selectToken(Text service, Collection<Token<? extends TokenIdentifier>> tokens) {
        Optional<Token<? extends TokenIdentifier>> sasl_token = saslToken(service, tokens);
        if (sasl_token.isPresent()) {
            // auth ok
            return (Token<SaslTokenIdentifier>) sasl_token.get();
        }

        LOGGER.info("no authorized token for server:" + service + " tokens:\n" + tokens.stream().map(Objects::toString).collect(Collectors.joining("\n")));
        if (SaslSecurityInfo.justlog()) {
            LOGGER.info("token auth not enabled, set " + SaslSecurityInfo.JUST_LOG_FAIL_AUTH + " to true to enable strick check");
            return newToken(service);
        }
        return null;
    }

    protected Token<SaslTokenIdentifier> newToken(Text service) {
        SaslTokenIdentifier identifier = new SaslTokenIdentifier();
        return new Token(identifier.getBytes(), "".getBytes(), identifier.getKind(), service);
    }
}
