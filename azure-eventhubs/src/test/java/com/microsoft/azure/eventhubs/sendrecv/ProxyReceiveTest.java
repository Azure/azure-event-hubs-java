package com.microsoft.azure.eventhubs.sendrecv;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.TransportType;
import com.microsoft.azure.eventhubs.lib.SasTokenTestBase;
import com.microsoft.azure.eventhubs.lib.TestContext;
import org.jutils.jproxy.ProxyServer;
import org.junit.*;

public class ProxyReceiveTest extends SasTokenTestBase {

    private static int proxyPort = 8899;
    private static ProxyServer proxyServer;
    private static ReceiveTest receiveTest;

    @BeforeClass
    public static void initialize() throws Exception {
        proxyServer = ProxyServer.create("localhost", proxyPort);
        proxyServer.start(t -> {});
        EventHubClient.setProxyHostName("localhost");
        EventHubClient.setProxyHostPort(proxyPort);

        Assert.assertTrue(TestContext.getConnectionString().getSharedAccessSignature() != null
                && TestContext.getConnectionString().getSasKey() == null
                && TestContext.getConnectionString().getSasKeyName() == null);

        receiveTest = new ReceiveTest();
        ConnectionStringBuilder connectionString = TestContext.getConnectionString();
        connectionString.setTransportType(TransportType.AMQP_WEB_SOCKETS);
        ReceiveTest.initializeEventHub(connectionString);
    }

    @AfterClass()
    public static void cleanup() throws Exception {
        ReceiveTest.cleanup();

        if (proxyServer != null) {
            proxyServer.stop();
        }
    }

    @Test()
    public void testReceiverStartOfStreamFilters() throws EventHubException {
        receiveTest.testReceiverStartOfStreamFilters();
    }

    @After
    public void testCleanup() throws EventHubException {
        receiveTest.testCleanup();
    }
}