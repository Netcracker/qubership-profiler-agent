package org.springframework.web.servlet.mvc.method.annotation;

import com.netcracker.profiler.agent.Profiler;
import com.netcracker.profiler.agent.http.HttpServletLogUtils;
import com.netcracker.profiler.agent.http.ServletRequestAdapter;

import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.connector.ResponseFacade;
import org.springframework.http.server.ServletServerHttpResponse;

import java.lang.reflect.Field;

import jakarta.servlet.http.HttpServletResponse;

public class ReactiveTypeHandler {

    private static transient Field emitterField$profiler;
    private static transient Field handlerField$profiler;
    private static transient Field outputMessageField$profiler;
    private static transient Field delegateField$profiler;
    private static transient Field responseField$profiler;

    /**
     * Flow how to get request from Spring reactive classes:
     * AbstractEmitterSubscriber
     *   -> emitter (field) -> handler (field) -> outputMessage (field) -> delegate (field) -> springResponse
     *       -> HttpServletResponse -> response (field) -> Request
     */
    @SuppressWarnings("UnusedNestedClass")
    private static abstract class AbstractEmitterSubscriber {
        @SuppressWarnings("EffectivelyPrivate")
        public void run$profiler(Throwable t) throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
            Field emitterField = emitterField$profiler;
            if (emitterField == null) {
                try {
                    emitterField = this.getClass().getSuperclass().getDeclaredField("emitter");
                } catch (NoSuchFieldException e) {
                    return;
                }
                emitterField$profiler = emitterField;
            }
            emitterField.setAccessible(true);
            Object emitter = emitterField.get(this);
            if (emitter == null) {
                return;
            }

            Field handlerField = handlerField$profiler;
            if (handlerField == null) {
                try {
                    handlerField = emitter.getClass().getDeclaredField("handler");
                } catch (NoSuchFieldException e) {
                    handlerField = emitter.getClass().getSuperclass().getDeclaredField("handler");
                }
                handlerField$profiler = handlerField;
            }
            handlerField.setAccessible(true);
            Object handler = handlerField.get(emitter);
            if (handler == null) {
                return;
            }

            Field outputMessageField = outputMessageField$profiler;
            if (outputMessageField == null) {
                try {
                    outputMessageField = handler.getClass().getDeclaredField("outputMessage");
                } catch (NoSuchFieldException e) {
                    return;
                }
                outputMessageField$profiler = outputMessageField;
            }
            outputMessageField.setAccessible(true);
            Object outputMessage = outputMessageField.get(handler);
            if (outputMessage == null) {
                return;
            }

            ServletServerHttpResponse springResponse = null;
            Class streamingServletServerHttpResponseClass = Class.forName(
                    "org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitterReturnValueHandler$StreamingServletServerHttpResponse"
            );
            if (streamingServletServerHttpResponseClass.isAssignableFrom(outputMessage.getClass())) {
                Field delegateField = delegateField$profiler;
                if (delegateField == null) {
                    try {
                        delegateField = outputMessage.getClass().getDeclaredField("delegate");
                    } catch (NoSuchFieldException e) {
                        return;
                    }
                    delegateField$profiler = delegateField;
                }
                delegateField.setAccessible(true);
                springResponse = (ServletServerHttpResponse) delegateField.get(outputMessage);
            } else {
                springResponse = (ServletServerHttpResponse) outputMessage;
            }

            HttpServletResponse jakataResponse = springResponse.getServletResponse();
            if (jakataResponse instanceof ResponseFacade) {
                Field responseField = responseField$profiler;
                if (responseField == null) {
                    try {
                        responseField = jakataResponse.getClass().getDeclaredField("response");
                    } catch (NoSuchFieldException e) {
                        return;
                    }
                    responseField$profiler = responseField;
                }
                responseField.setAccessible(true);
                jakataResponse = (HttpServletResponse) responseField.get(jakataResponse);
            }
            if (jakataResponse instanceof Response) {
                Request request = ((Response) jakataResponse).getRequest();
                try {
                    ServletRequestAdapter adapter = new ServletRequestAdapter(request);
                    HttpServletLogUtils.dumpRequest(adapter);
                    HttpServletLogUtils.afterRequest(adapter);
                    HttpServletLogUtils.fillNcUser(adapter);
                } catch (Throwable e) {
                    Profiler.pluginException(e);
                }
            }
        }
    }
}
