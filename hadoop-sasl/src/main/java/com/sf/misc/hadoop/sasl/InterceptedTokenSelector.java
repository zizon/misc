package com.sf.misc.hadoop.sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

import java.util.Collection;
import java.util.Optional;

public class InterceptedTokenSelector extends SaslTokenSelector {

    public static final Log LOGGER = LogFactory.getLog(InterceptedTokenSelector.class);

    protected final TokenSelector selector;

    public InterceptedTokenSelector(TokenSelector selector) {
        this.selector = selector;
    }

    @Override
    public Token selectToken(Text service, Collection<Token<? extends TokenIdentifier>> tokens) {
        Optional<Token<?>> sasl_token = saslToken(service, tokens);
        if (sasl_token.isPresent()) {
            LOGGER.info("sasl token check ok");
            return selector.selectToken(service, tokens);
        }

        LOGGER.warn("no sasl token found, check fail");
        if (!SaslSecurityInfo.strictCheck()) {
            LOGGER.warn("sasl token strict check not enable, just log fail auth,set " + SaslSecurityInfo.STRICT_CHECK + " to enable strict check" );
            return selector.selectToken(service, tokens);
        }

        return null;
    }
}
