package com.microsoft.azure.eventhubs.sendrecv;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.TransportType;
import com.microsoft.azure.eventhubs.lib.SasTokenTestBase;
import com.microsoft.azure.eventhubs.lib.TestContext;
import org.jutils.jproxy.ProxyServer;
import org.junit.*;

import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ProxySendTest extends SasTokenTestBase {

    private static int proxyPort = 8899;
    private static ProxyServer proxyServer;
    private static SendTest sendTest;

    @BeforeClass
    public static void initialize() throws Exception {
        proxyServer = ProxyServer.create("localhost", proxyPort);
        proxyServer.start(t -> {});
        EventHubClient.setProxyHostName("localhost");
        EventHubClient.setProxyHostPort(proxyPort);

        Assert.assertTrue(TestContext.getConnectionString().getSharedAccessSignature() != null
                && TestContext.getConnectionString().getSasKey() == null
                && TestContext.getConnectionString().getSasKeyName() == null);

        sendTest = new SendTest();

        ConnectionStringBuilder connectionString = TestContext.getConnectionString();
        connectionString.setTransportType(TransportType.AMQP_WEB_SOCKETS);
        SendTest.initializeEventHub(connectionString);
    }

    @AfterClass
    public static void cleanupClient() throws Exception {

        SendTest.cleanupClient();

        if (proxyServer != null) {
            proxyServer.stop();
        }
    }

    @Test
    public void sendBatchRetainsOrderWithinBatch() throws EventHubException, InterruptedException, ExecutionException, TimeoutException {

        sendTest.sendBatchRetainsOrderWithinBatch();
    }

    @Test
    public void sendResultsInSysPropertiesWithPartitionKey() throws EventHubException, InterruptedException, ExecutionException, TimeoutException {

        sendTest.sendResultsInSysPropertiesWithPartitionKey();
    }

    @After
    public void cleanup() throws Exception {

        sendTest.cleanup();
    }
}
