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

import java.net.*;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProxyConnectionHandler extends WebSocketConnectionHandler {
    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(ProxyConnectionHandler.class);
    private final String proxySelectorModifiedError = "ProxySelector has been modified.";

    public static Boolean shouldUseProxy(final String hostName) {
        final URI uri = createURIFromHostNamePort(hostName, ClientConstants.HTTPS_PORT);
        final ProxySelector proxySelector = ProxySelector.getDefault();
        if (proxySelector == null) {
            return false;
        }

        final List<Proxy> proxies = proxySelector.select(uri);
        return isProxyAddressLegal(proxies);
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
        final InetSocketAddress socketAddress = getProxyAddress();
        return socketAddress.getHostString();
    }

    @Override
    public int getOutboundSocketPort() {
        final InetSocketAddress socketAddress = getProxyAddress();
        return socketAddress.getPort();
    }

    private Map<String, String> getAuthorizationHeader() {
        final PasswordAuthentication authentication = Authenticator.requestPasswordAuthentication(
                getOutboundSocketHostName(),
                null,
                getOutboundSocketPort(),
                null,
                null,
                "http",
                null,
                Authenticator.RequestorType.PROXY);
        if (authentication == null) {
            return null;
        }

        final String proxyUserName = authentication.getUserName();
        final String proxyPassword = authentication.getPassword() != null
                ? new String(authentication.getPassword())
                : null;
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

    private InetSocketAddress getProxyAddress() {
        final URI serviceUri = createURIFromHostNamePort(
                this.getMessagingFactory().getHostName(),
                this.getProtocolPort());
        final ProxySelector proxySelector = ProxySelector.getDefault();
        if (proxySelector == null) {
            throw new IllegalStateException(proxySelectorModifiedError);
        }

        final List<Proxy> proxies = proxySelector.select(serviceUri);
        if (!isProxyAddressLegal(proxies)) {
            throw new IllegalStateException(proxySelectorModifiedError);
        }

        final Proxy proxy = proxies.get(0);
        return (InetSocketAddress) proxy.address();
    }

    private static URI createURIFromHostNamePort(final String hostName, final int port) {
        return URI.create(String.format(ClientConstants.HTTPS_URI_FORMAT, hostName, port));
    }

    private static boolean isProxyAddressLegal(final List<Proxy> proxies) {
        // we look only at the first proxy in the list
        // if the proxy can be translated to InetSocketAddress
        // only then - can we parse it to hostName and Port
        // which is required by qpid-proton-j library reactor.connectToHost() API
        return proxies != null
                && !proxies.isEmpty()
                && proxies.get(0).type() == Proxy.Type.HTTP
                && proxies.get(0).address() != null
                && proxies.get(0).address() instanceof InetSocketAddress;
    }
}
