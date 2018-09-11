/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.impl;

import com.microsoft.azure.proton.transport.proxy.ProxyHandler;
import com.microsoft.azure.proton.transport.proxy.impl.ProxyHandlerImpl;
import com.microsoft.azure.proton.transport.proxy.impl.ProxyImpl;

import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.impl.TransportInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class ProxyConnectionHandler extends WebSocketConnectionHandler {
    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(ProxyConnectionHandler.class);

    public static Boolean shouldUseProxy() {
        return !StringUtil.isNullOrEmpty(EventHubClientImpl.proxyHostName);
    }

    public ProxyConnectionHandler(AmqpConnection messagingFactory) {
        super(messagingFactory);
    }

    @Override
    protected void addTransportLayers(final Event event, final TransportInternal transport) {
        super.addTransportLayers(event, transport);

        final ProxyImpl proxy = new ProxyImpl();

        // host name used to create proxy connect request
        // after creating the socket to proxy
        final String hostName = event.getConnection().getHostname();
        final ProxyHandler proxyHandler = new ProxyHandlerImpl();
        final Map<String, String> proxyHeader = getAuthorizationHeader();
        proxy.configure(hostName, proxyHeader, proxyHandler, transport);

        transport.addTransportLayer(proxy);

        if (TRACE_LOGGER.isInfoEnabled()) {
            TRACE_LOGGER.info("addProxyHandshake: hostname[" + hostName +"]");
        }
    }

    @Override
    public String getOutboundSocketHostName() {
        return EventHubClientImpl.proxyHostName;
    }

    @Override
    public int getOutboundSocketPort() {
        return EventHubClientImpl.proxyHostPort;
    }

    private Map<String, String> getAuthorizationHeader() {
        final String proxyUserName = EventHubClientImpl.proxyUserName;
        final String proxyPassword = EventHubClientImpl.proxyPassword;
        if (StringUtil.isNullOrEmpty(proxyUserName)
                || StringUtil.isNullOrEmpty(proxyPassword)) {
            return null;
        }

        final HashMap<String, String> proxyAuthorizationHeader = new HashMap<>();
        // https://tools.ietf.org/html/rfc7617
        final String usernamePasswordPair = proxyUserName + ":" + proxyPassword;
        proxyAuthorizationHeader.put(
                "Proxy-Authorization",
                "Basic " + Base64.getEncoder().encodeToString(usernamePasswordPair.getBytes()));
        return proxyAuthorizationHeader;
    }
}
