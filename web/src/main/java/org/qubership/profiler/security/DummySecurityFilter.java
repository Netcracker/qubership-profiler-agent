package org.qubership.profiler.security;

import static org.qubership.profiler.security.SecurityConstants.AUTHENTICATED_USER;
import static org.qubership.profiler.security.SecurityConstants.LAST_USER_URI;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class DummySecurityFilter extends AbstractSecurityFilter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }


//    @Override
//    public void doFilter(ServletRequest request, ServletResponse response,
//                         FilterChain filterChain) throws IOException, ServletException {
//        if (request instanceof HttpServletRequest) {
////            HttpServletRequest httpRequest = (HttpServletRequest) request;
////            Cookie[] cookies = httpRequest.getCookies();
//            HttpSession session = ((HttpServletRequest) request).getSession();
//            User user = (User) session.getAttribute(SecurityConstants.AUTHENTICATED_USER);
//            if (user == null) {
//                HttpServletResponse httpResponse = (HttpServletResponse) response;
//                httpResponse.sendRedirect("/login.html");
//            }
//        }
//        filterChain.doFilter(request, response);
//    }

    @Override
    protected void doFilter(HttpServletRequest httpRequest, HttpServletResponse httpResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpSession session = httpRequest.getSession();
        User user = (User) session.getAttribute(AUTHENTICATED_USER);
        String path = httpRequest.getRequestURI();
        if (user == null && !path.startsWith("/login.html")) {
            if (path.startsWith("/index.html") || path.startsWith("/tree.html")) {
                session.setAttribute(LAST_USER_URI, path + "?" + httpRequest.getQueryString());
            }
            httpResponse.sendRedirect("/login.html");
        } else {
            filterChain.doFilter(httpRequest, httpResponse);
        }
    }

//    private Cookie jSessionCookie(Cookie[] cookies) {
//        Cookie jSessionCookie = null;
//        for (Cookie cookie : cookies) {
//            if (cookie.getName().equalsIgnoreCase("jsessionId")) {
//                jSessionCookie = cookie;
//                break;
//            }
//        }
//        return jSessionCookie;
//    }

    @Override
    public void destroy() {

    }
}
