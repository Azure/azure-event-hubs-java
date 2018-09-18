package com.microsoft.azure.eventhubs.lib.proxy;

import com.microsoft.azure.eventhubs.lib.proxy.impl.SimpleProxy;

import java.io.IOException;
import java.util.function.Consumer;

public interface ProxyServer {

    static ProxyServer create(final String hostName, final int port) {
        return new SimpleProxy(hostName, port);
    }

    void start(final Consumer<Throwable> onError) throws IOException;

    void stop() throws IOException;
}
