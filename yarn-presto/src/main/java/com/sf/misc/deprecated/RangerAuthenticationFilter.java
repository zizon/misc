package com.sf.misc.deprecated;

import io.airlift.log.Logger;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;

public class RangerAuthenticationFilter implements Filter {

    public static final Logger LOGGER = Logger.get(RangerAuthenticationFilter.class);

    protected boolean authenticate(HttpServletRequest request) {
        if (!request.getRequestURI().startsWith("/v1/statement")) {
            return true;
        }

        //TODO
        String header = request.getHeader(AUTHORIZATION);
        LOGGER.info("get header:" + header);
        return true;
    }


    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest http = ((HttpServletRequest) request);
        if (!authenticate(http)) {
            ((HttpServletResponse) response).sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
}
