package com.microsoft.azure.eventhubs.exceptioncontracts;

import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.eventhubs.lib.ApiTestBase;
import com.microsoft.azure.eventhubs.lib.TestContext;
import com.microsoft.azure.eventhubs.lib.proxy.ProxyServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class ProxySendLargeMessageTest extends ApiTestBase {
    private static int proxyPort = 8899;
    private static ProxyServer proxyServer;
    private static SendLargeMessageTest sendLargeMessageTest;

    @BeforeClass
    public static void initialize() throws Exception {
        proxyServer = ProxyServer.create("localhost", proxyPort);
        proxyServer.start(t -> {});
        EventHubClient.setProxyHostName("localhost");
        EventHubClient.setProxyHostPort(proxyPort);
        final ConnectionStringBuilder connectionStringBuilder = TestContext.getConnectionString();
        connectionStringBuilder.setTransportType(TransportType.AMQP_WEB_SOCKETS);
        sendLargeMessageTest = new SendLargeMessageTest();
        SendLargeMessageTest.initializeEventHubClients(connectionStringBuilder);
    }

    @AfterClass()
    public static void cleanup() throws Exception {
        SendLargeMessageTest.cleanup();

        if (proxyServer != null) {
            proxyServer.stop();
        }
    }

    @Test()
    public void sendMsgLargerThan64k() throws EventHubException, InterruptedException, ExecutionException, IOException {
        sendLargeMessageTest.sendMsgLargerThan64k();
    }

    @Test(expected = PayloadSizeExceededException.class)
    public void sendMsgLargerThan256K() throws EventHubException, InterruptedException, ExecutionException, IOException {
        sendLargeMessageTest.sendMsgLargerThan256K();
    }

    @Test()
    public void sendMsgLargerThan128k() throws EventHubException, InterruptedException, ExecutionException, IOException {
        sendLargeMessageTest.sendMsgLargerThan128k();
    }
}