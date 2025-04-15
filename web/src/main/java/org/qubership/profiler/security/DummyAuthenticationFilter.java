package org.qubership.profiler.security;

import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.qubership.profiler.security.SecurityConstants.AUTHENTICATED_USER;
import static org.qubership.profiler.security.SecurityConstants.LAST_USER_URI;
import static org.qubership.profiler.security.SecurityConstants.USER_NAME_PARAMETER;
import static org.qubership.profiler.security.SecurityConstants.USER_PASSWORD_PARAMETER;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class DummyAuthenticationFilter extends AbstractSecurityFilter {

    private static final Logger logger = LoggerFactory.getLogger(DummyAuthenticationFilter.class);

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
                User currentUser = DummySecurityService.getInstance().tryAuthenticate(userName, userPassword);
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
