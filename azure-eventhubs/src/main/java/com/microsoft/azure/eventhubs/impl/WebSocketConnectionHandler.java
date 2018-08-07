package com.microsoft.azure.eventhubs.impl;

import com.microsoft.azure.proton.transport.ws.impl.WebSocketImpl;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.impl.TransportInternal;

public class WebSocketConnectionHandler extends ConnectionHandler {
    public WebSocketConnectionHandler(AmqpConnection messagingFactory) {
        super(messagingFactory);
    }

    @Override
    protected void addTransportLayers(final Event event, final TransportInternal transport) {
        final WebSocketImpl webSocket = new WebSocketImpl();
        webSocket.configure(
                event.getConnection().getHostname(),
                "/$servicebus/websocket",
                null,
                0,
                "AMQPWSB10",
                null,
                null);

        transport.addTransportLayer(webSocket);
    }

    @Override
    protected int getPort() {
        return ClientConstants.HTTPS_PORT;
    }

    @Override
    protected int getMaxFrameSize() {
        return 4 * 1024;
    }
}
