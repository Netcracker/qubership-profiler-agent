package com.netcracker.profiler.security.csrf;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class CSRFGuardFilter implements Filter {
    private static final Logger log = LoggerFactory.getLogger(CSRFGuardFilter.class);

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        final HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;

        final String uri = request.getRequestURI();

        boolean redirected;
        final HttpSession session = request.getSession();
        redirected = checkCSRF(uri, request, response, session);
        if (!redirected) {
            filterChain.doFilter(request, response);
        }
    }

    private boolean checkCSRF(final String uri, final HttpServletRequest request, final HttpServletResponse response,
                              HttpSession session) throws IOException {
        if (request.getMethod().equalsIgnoreCase("GET")) {
            return false;
        }
        String tokenFromRequest = request.getParameter(CSRFGuardHelper.CSRF_TOKEN_P);
        if (tokenFromRequest == null) {
            tokenFromRequest = request.getHeader(CSRFGuardHelper.CSRF_TOKEN_P);
        }
        if (StringUtils.isEmpty(tokenFromRequest)) {
            log.error("CSRF token is empty for {} at {}", request.getMethod(), uri);
            redirect(response);
            return true;
        }
        Object csrfTokenObj = session.getAttribute(CSRFGuardHelper.CSRF_TOKEN_P);
        if (csrfTokenObj == null) {
            log.error("The session does not contain CSRF token while client sent some for {} at {}", request.getMethod(), uri);
            redirect(response);
            return true;
        }
        String tokenValue_p = csrfTokenObj.toString();
        if (!tokenFromRequest.equals(tokenValue_p)) {
            log.error("CSRF token in request does not match the token stored in session, {} at {}", request.getMethod(), uri);
            redirect(response);
            return true;
        }
        return false;
    }


    private void redirect(final ServletResponse response) throws IOException {
        final HttpServletResponse httpResponse = (HttpServletResponse) response;
        httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN); //internally cserror.jsp will run for 403 status code
    }

    @Override
    public void destroy() {

    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }
}
