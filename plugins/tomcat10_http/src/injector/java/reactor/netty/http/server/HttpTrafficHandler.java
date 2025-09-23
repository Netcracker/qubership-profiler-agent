package reactor.netty.http.server;

import com.netcracker.profiler.agent.Profiler;

import io.netty.handler.codec.http.HttpRequest;

import java.lang.reflect.InvocationTargetException;

public class HttpTrafficHandler {
    public void channelRead$profiler(io.netty.channel.ChannelHandlerContext ctx, Object msg, Throwable t) throws InvocationTargetException, IllegalAccessException {
        if (!(msg instanceof HttpRequest)) {
            return;
        }

        HttpRequest req = (HttpRequest) msg;
        String uri = req.uri();
        String localAddress = ctx.channel().localAddress().toString();
        Profiler.event(localAddress + uri, "web.url");

        String method = req.method().toString();
        Profiler.event(method, "web.method");

        String remoteAddress = ctx.channel().remoteAddress().toString();
        Profiler.event(remoteAddress, "web.remote.addr");
        Profiler.event(remoteAddress, "web.remote.host");
    }
}
