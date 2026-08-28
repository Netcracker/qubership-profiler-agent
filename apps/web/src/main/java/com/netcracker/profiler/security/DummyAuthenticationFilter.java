package com.netcracker.profiler.security;

import static com.netcracker.profiler.security.SecurityConstants.AUTHENTICATED_USER;
import static com.netcracker.profiler.security.SecurityConstants.LAST_USER_URI;
import static com.netcracker.profiler.security.SecurityConstants.USER_NAME_PARAMETER;
import static com.netcracker.profiler.security.SecurityConstants.USER_PASSWORD_PARAMETER;
import static com.netcracker.profiler.util.StringUtils.isNotEmpty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

@Singleton
public class DummyAuthenticationFilter extends AbstractSecurityFilter {

    private static final Logger logger = LoggerFactory.getLogger(DummyAuthenticationFilter.class);

    @Inject
    public DummyAuthenticationFilter(DummySecurityService securityService) {
        super(securityService);
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    protected void doFilter(HttpServletRequest httpRequest, HttpServletResponse httpResponse,
                            FilterChain filterChain) throws IOException, ServletException {
        HttpSession session = httpRequest.getSession();
        String userName = httpRequest.getParameter(USER_NAME_PARAMETER);
        String userPassword = httpRequest.getParameter(USER_PASSWORD_PARAMETER);
        try {
            if (isNotEmpty(userName) && isNotEmpty(userPassword)) {
                User currentUser = securityService.tryAuthenticate(userName, userPassword);
                session.setAttribute(AUTHENTICATED_USER, currentUser);
                String redirectTo = (String) session.getAttribute(LAST_USER_URI);
                session.removeAttribute(redirectTo);
                httpResponse.sendRedirect(redirectTo != null ? redirectTo : "/index.html");
                logger.debug("User {} successfully authenticated", userName);
            }
        } catch (WinstoneAuthException e) {
//            httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
        filterChain.doFilter(httpRequest, httpResponse);

    }
}
