/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.connstrbuilder;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.TransportType;
import com.microsoft.azure.eventhubs.impl.ConnectionHandler;
import com.microsoft.azure.eventhubs.impl.EventHubClientImpl;
import com.microsoft.azure.eventhubs.impl.MessagingFactory;
import com.microsoft.azure.eventhubs.lib.ApiTestBase;
import com.microsoft.azure.eventhubs.lib.TestContext;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class TransportTypeTest extends ApiTestBase {

    static EventHubClient ehClient;

    @AfterClass
    public static void cleanupClient() throws EventHubException {
        if (ehClient != null) {
            ehClient.closeSync();
        }
    }

    @Test
    public void transportTypeAmqpCreatesConnectionWithPort5671() throws Exception {
        ConnectionStringBuilder builder = new ConnectionStringBuilder(TestContext.getConnectionString().toString());
        builder.setTransportType(TransportType.AMQP);

        ehClient = EventHubClient.createSync(builder.toString(), TestContext.EXECUTOR_SERVICE);

        EventHubClientImpl eventHubClientImpl = (EventHubClientImpl) ehClient;
        final Field factoryField = EventHubClientImpl.class.getDeclaredField("underlyingFactory");
        factoryField.setAccessible(true);
        final MessagingFactory underlyingFactory = (MessagingFactory) factoryField.get(eventHubClientImpl);

        final Field connectionHandlerField = MessagingFactory.class.getDeclaredField("connectionHandler");
        connectionHandlerField.setAccessible(true);
        final ConnectionHandler connectionHandler = (ConnectionHandler) connectionHandlerField.get(underlyingFactory);

        final Method outboundSocketPort = ConnectionHandler.class.getDeclaredMethod("getOutboundSocketPort");
        outboundSocketPort.setAccessible(true);

        final Method protocolPort = ConnectionHandler.class.getDeclaredMethod("getProtocolPort");
        protocolPort.setAccessible(true);

        Assert.assertEquals(5671, outboundSocketPort.invoke(connectionHandler));
        Assert.assertEquals(5671, protocolPort.invoke(connectionHandler));
    }

    @Test
    public void transportTypeAmqpCreatesConnectionWithPort443() throws Exception {
        ConnectionStringBuilder builder = new ConnectionStringBuilder(TestContext.getConnectionString().toString());
        builder.setTransportType(TransportType.AMQP_WEB_SOCKETS);

        ehClient = EventHubClient.createSync(builder.toString(), TestContext.EXECUTOR_SERVICE);

        EventHubClientImpl eventHubClientImpl = (EventHubClientImpl) ehClient;
        final Field factoryField = EventHubClientImpl.class.getDeclaredField("underlyingFactory");
        factoryField.setAccessible(true);
        final MessagingFactory underlyingFactory = (MessagingFactory) factoryField.get(eventHubClientImpl);

        final Field connectionHandlerField = MessagingFactory.class.getDeclaredField("connectionHandler");
        connectionHandlerField.setAccessible(true);
        final ConnectionHandler connectionHandler = (ConnectionHandler) connectionHandlerField.get(underlyingFactory);

        final Method outboundSocketPort = ConnectionHandler.class.getDeclaredMethod("getOutboundSocketPort");
        outboundSocketPort.setAccessible(true);

        final Method protocolPort = ConnectionHandler.class.getDeclaredMethod("getProtocolPort");
        protocolPort.setAccessible(true);

        Assert.assertEquals(443, outboundSocketPort.invoke(connectionHandler));
        Assert.assertEquals(443, protocolPort.invoke(connectionHandler));
    }
}