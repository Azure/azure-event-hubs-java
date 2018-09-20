/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.proxy;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.TransportType;
import com.microsoft.azure.eventhubs.impl.ConnectionHandler;
import com.microsoft.azure.eventhubs.impl.EventHubClientImpl;
import com.microsoft.azure.eventhubs.impl.MessagingFactory;
import com.microsoft.azure.eventhubs.lib.ApiTestBase;
import com.microsoft.azure.eventhubs.lib.TestContext;
import org.junit.Assert;
import org.junit.Test;
import org.jutils.jproxy.ProxyServer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.*;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

public class ProxySelectorTest extends ApiTestBase {
    private volatile boolean isProxyConnectFailedInvoked = false;
    @Test
    public void proxySelectorConnectFailedInvokeTest() throws Exception {
        int proxyPort = 8899;
        this.isProxyConnectFailedInvoked = false;
        ProxySelector.setDefault(new ProxySelector() {
            @Override
            public List<Proxy> select(URI uri) {
                LinkedList<Proxy> proxies = new LinkedList<>();
                proxies.add(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", proxyPort)));
                return proxies;
            }

            @Override
            public void connectFailed(URI uri, SocketAddress sa, IOException ioe) {
                ProxySelectorTest.this.isProxyConnectFailedInvoked = true;
            }
        });

        ConnectionStringBuilder builder = new ConnectionStringBuilder(TestContext.getConnectionString().toString());
        builder.setTransportType(TransportType.AMQP_WEB_SOCKETS);
        builder.setOperationTimeout(Duration.ofSeconds(10));

        try {
            EventHubClient.createSync(builder.toString(), TestContext.EXECUTOR_SERVICE);
            Assert.assertTrue(false); // shouldn't reach here
        } catch(EventHubException ex) {
            System.out.println(ex.getMessage());
        }

        Assert.assertTrue(isProxyConnectFailedInvoked);
    }
}
