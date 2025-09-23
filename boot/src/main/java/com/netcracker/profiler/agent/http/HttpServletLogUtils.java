package com.netcracker.profiler.agent.http;

import com.netcracker.profiler.agent.*;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class HttpServletLogUtils {

    private static final String LOG_HEADERS_PROPERTY = "events.http.headers";
    private static Set<String> headersToLog;

    static {
        ProfilerProperty profilerProperty = ProfilerData.properties.get(LOG_HEADERS_PROPERTY);
        if(profilerProperty != null) {
            headersToLog = profilerProperty.getMultipleValues();
        } else {
            headersToLog = Collections.EMPTY_SET;
        }
    }

    public static void fillNcUser(ServletRequestAdapter request) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final CallInfo callInfo = Profiler.getState().callInfo;
        if (!(request.isHttpServetRequest())) {
            return;
        }
        if (callInfo.getNcUser() != null && !"null".equals(callInfo.getNcUser())) {
            return;
        }
        HttpServletRequestAdapter req = request.toHttpServletRequestAdapter();

        final HttpSessionAdapter session = req.getSession(false);
        if (session == null) {
            return;
        }
        if (callInfo.getNcUser() == null) {
            Object springSecurityContext;
            try {
                springSecurityContext = session.getAttribute("SPRING_SECURITY_CONTEXT");
            } catch (Exception e) {
                return; //The session is invalid
            }

            if (springSecurityContext != null) {
                Method getAuthentication;
                try {
                    getAuthentication = springSecurityContext.getClass().getMethod("getAuthentication");
                    Object authentication = getAuthentication.invoke(springSecurityContext);
                    if (authentication != null) {
                        Method getPrincipal = authentication.getClass().getMethod("getPrincipal");
                        Object principal = getPrincipal.invoke(authentication);
                        if (principal != null) {
                            Method getUsername = principal.getClass().getMethod("getUsername");
                            Object user = getUsername.invoke(principal);
                            if (user != null) {
                                callInfo.setNcUser(String.valueOf(user));
                            }
                        }
                    }
                } catch (NoSuchMethodException e) {
                } catch (InvocationTargetException e) {
                } catch (IllegalAccessException e) {
                }
            }
        }
        if (callInfo.getNcUser() != null && !"null".equals(callInfo.getNcUser())){
            dumpNCUserInfo(request.getRemoteAddr(), callInfo);
        }
    }

    public static void dumpRequest(ServletRequestAdapter request) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final CallInfo callInfo = Profiler.getState().callInfo;
        if (request.isHttpServetRequest()) {
            HttpServletRequestAdapter req = request.toHttpServletRequestAdapter();
            {

                String url = req.getRequestURL().toString();

                Profiler.event(url, "web.url");
                if (url != null) {
                    callInfo.setRequestURL(url);

                    url = StringUtils.right(url, CallInfo.MODULE_LENGTH - 2);
                    url = req.getMethod().substring(0, 1) + ' ' + url;
                    callInfo.setModule(url);
                }
            }
            {
                String query = req.getQueryString();
                if (query != null && query.length() > 0) {
                    Profiler.event(query, "web.query");
                    callInfo.setAction(query);
                }
            }
            dumpSessionInfo(req, callInfo);

            Profiler.event(req.getRequestedSessionId(), "web.session.id");
            Profiler.event(req.getMethod(), "web.method");
            Profiler.event(req.getHeader("Referer"), "_web.referer");
            Profiler.event(req.getHeader("dynaTrace"), "dynatrace");
            Profiler.event(req.getHeader("X-JMeter-Step"), "jmeter.step");

            Profiler.event(req.getHeader("x-request-id"), "x-request-id");
            String clientTransactionId = req.getHeader("x-client-transaction-id");
            Profiler.event(clientTransactionId, "x-client-transaction-id");
            String traceId = req.getHeader("X-B3-TraceId");
            Profiler.event(traceId, "X-B3-TraceId");

            Profiler.event(req.getHeader("X-B3-SpanId"), "X-B3-SpanId");
            Profiler.event(req.getHeader("X-B3-ParentSpanId"), "X-B3-ParentSpanId");

            for(String header : headersToLog) {
                Profiler.event(req.getHeader(header), header);
            }

            String endToEndId = req.getHeader("X-EndToEndId");
            CookieAdapter[] cookies = req.getCookies();
            if ((endToEndId == null || endToEndId.length() == 0) && cookies != null) {
                for (CookieAdapter cookie : cookies) {
                    if ("nc.esc.tid".equals(cookie.getName())) {
                        endToEndId = cookie.getValue();
                        break;
                    }
                    if (!"PT".equals(cookie.getName())) continue;
                    String pt = cookie.getValue();
                    Map<String, String> params = new LinkedHashMap<String, String>();
                    String[] pairs = pt.split("&");
                    for (String pair : pairs) {
                        int idx = pair.indexOf("=");
                        try {
                            params.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            /* must not happen */
                        }
                    }
                    String scenario = params.get("sn.scenario");
                    String source = params.get("pt.source");
                    if (source == null) break;
                    if (source.length() > 40)
                        source = source.substring(0, 40);
                    if (scenario != null)
                        source = scenario + "_" + source;
                    String start = params.get("pt.start");
                    String uid = params.get("sn.uid");
                    endToEndId = source + "#" + start;
                    if (uid != null) endToEndId += "#" + uid;
                    break;
                }
            }
            if (endToEndId == null && traceId != null) {
                endToEndId = traceId;
            }
            if (endToEndId == null && clientTransactionId != null) {
                endToEndId = clientTransactionId;
            }
            if (endToEndId != null && endToEndId.length() > 0) {
                callInfo.setEndToEndId(endToEndId);
            }
        }
        {
            final String remoteAddr = request.getRemoteAddr();
            Profiler.event(remoteAddr, "web.remote.addr");
            callInfo.setRemoteAddress(remoteAddr);

            dumpNCUserInfo(remoteAddr, callInfo);
        }
        Profiler.event(request.getRemoteHost(), "web.remote.host");
    }

    public static void dumpNCUserInfo(String remoteAddr, CallInfo callInfo) {
        StringBuilder sb = new StringBuilder(25);
        String userName = callInfo.getNcUser();
        if (userName != null) {
            Profiler.event(userName, "nc.user");
            sb.append(userName);
        }
        sb.append('@').append(remoteAddr);
        callInfo.setCliendId(sb.toString());
    }

    public static void afterRequest(ServletRequestAdapter request) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        final CallInfo callInfo = Profiler.getState().callInfo;

        if (callInfo.getEndToEndId() != null && callInfo.getEndToEndId().length() != 0) {
            request.setAttribute("x-nc-trace-id", callInfo.getEndToEndId());
        }

        if (callInfo.getNcUser() == null && request.isHttpServetRequest()) {
            dumpSessionInfo( request.toHttpServletRequestAdapter(), callInfo);
        }
    }

//    public static void afterRequest() {
//        final CallInfo callInfo = Profiler.getState().callInfo;
//
//        if (callInfo.getEndToEndId() != null && callInfo.getEndToEndId().length() != 0) {
//            request.setAttribute("x-nc-trace-id", callInfo.getEndToEndId());
//        }
//
//        if (callInfo.getNcUser() == null && request.isHttpServetRequest()) {
//            dumpSessionInfo( request.toHttpServletRequestAdapter(), callInfo);
//        }
//    }

    public static void dumpSessionInfo(HttpServletRequestAdapter req, CallInfo callInfo) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        final HttpSessionAdapter session = req.getSession(false);
        if (session != null) {
            final Object userSession = session.getAttribute("usession");
            if (userSession != null) {
                Class<?> sessionClass = userSession.getClass();
                try {
                    final Method getUserName = sessionClass.getMethod("getUserName");
                    String ncUser = (String) getUserName.invoke(userSession);
                    callInfo.setNcUser(ncUser);
                    if (ncUser != null) {
                        req.setAttribute("x-nc-username", ncUser);
                    }

                    Method getId = sessionClass.getMethod("getID");
                    Object sessionId = getId.invoke(userSession);
                    if (sessionId != null) {
                        req.setAttribute("x-nc-session-id", String.valueOf(sessionId));
                    }
                } catch (Throwable e) {
                    /* ignore */
                }
            }
        }
    }
}
