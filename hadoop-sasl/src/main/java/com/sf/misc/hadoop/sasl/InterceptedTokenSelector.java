package com.sf.misc.hadoop.sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

import java.util.Collection;
import java.util.Optional;

public class InterceptedTokenSelector extends DefaultSaslTokenSelector {

    public static final Log LOGGER = LogFactory.getLog(InterceptedTokenSelector.class);

    protected final TokenSelector selector;

    public InterceptedTokenSelector(TokenSelector selector) {
        this.selector = selector;
    }

    @Override
    public Token selectToken(Text service, Collection<Token<? extends TokenIdentifier>> tokens) {
        Optional<Token<?>> sasl_token = saslToken(service, tokens);
        if (sasl_token.isPresent()) {
            LOGGER.info("sasl token cehck ok");
            return selector.selectToken(service, tokens);
        }

        LOGGER.warn("sasl token:" + sasl_token.orElse(null) + " check fail");
        return null;
    }
}
