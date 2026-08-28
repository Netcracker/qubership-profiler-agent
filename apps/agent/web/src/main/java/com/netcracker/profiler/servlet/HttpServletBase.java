package com.netcracker.profiler.servlet;

import com.netcracker.profiler.io.exceptions.ErrorCollector;
import com.netcracker.profiler.io.exceptions.ErrorSupervisor;
import com.netcracker.profiler.output.layout.Layout;

import java.io.IOException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public abstract class HttpServletBase<Mediator, Context> extends HttpServlet {
    protected long parseLong(HttpServletRequest request, String paramName, long defaultValue) throws IllegalArgumentException {
        final String s = request.getParameter(paramName);
        if (s == null || s.length() == 0)
            return defaultValue;
        try {
            return Long.parseLong(s);
        } catch (Throwable t) {
            return defaultValue;
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ErrorSupervisor supervisor = createSupervisor();
        ErrorSupervisor.push(supervisor);
        Context context = createContext();
        try {
            Layout layout = identifyLayout(context, req, resp);
            Mediator mediator = getMediator(context, req, resp, layout);
            Runnable action = identifyAction(context, req, resp, mediator);
            action.run();
        } finally {
            ErrorSupervisor.pop();
        }
    }

    protected ErrorSupervisor createSupervisor() {
        return new ErrorCollector();
    }

    protected Context createContext() {
        return null;
    }

    protected abstract Layout identifyLayout(Context context, HttpServletRequest req, HttpServletResponse resp);

    protected abstract Mediator getMediator(Context context, HttpServletRequest req, HttpServletResponse resp, Layout layout);

    protected abstract Runnable identifyAction(Context context, HttpServletRequest req, HttpServletResponse resp, Mediator mediator);
}
