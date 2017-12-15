package com.microsoft.azure.eventhubs.sendrecv;

import com.microsoft.aad.adal4j.AuthenticationCallback;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.eventhubs.AzureActiveDirectoryTokenProvider;
import com.microsoft.azure.eventhubs.AuthorizationFailedException;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.ITokenProvider;
import com.microsoft.azure.eventhubs.PartitionSender;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.SecurityToken;
import com.microsoft.azure.eventhubs.lib.ApiTestBase;
import com.microsoft.azure.eventhubs.lib.TestContext;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class AdalTest extends ApiTestBase {

    final ExecutorService executorService = Executors.newCachedThreadPool();

    final static String TENANT_ID = "----TENANT_ID----";
    final static String CLIENT_ID = "---CLIENT_ID----";
    final static String CLIENT_SECRET = "---CLIENT_SECRET---";

    final static String EVENTHUB_NAME = TestContext.getConnectionString().getEntityPath();
    final static String NAMESPACE_ENDPOINT = TestContext.getConnectionString().getEndpoint().getHost();

    // @Test
    public void runEventHubSendReceiveTest() throws Exception {

        final String testMessage = "somedata test";
        final AuthenticationContext authenticationContext = new AuthenticationContext(
                "https://login.windows.net/" + TENANT_ID,
                true,
                executorService);
        final ClientCredential clientCredential = new ClientCredential(
                CLIENT_ID,
                CLIENT_SECRET);
        final EventHubClient ehClient = EventHubClient.create(
                new URI("sb://" + NAMESPACE_ENDPOINT),
                EVENTHUB_NAME,
                authenticationContext,
                clientCredential).get();


        final PartitionReceiver pReceiver = ehClient.createReceiverSync("$Default", "0", PartitionReceiver.END_OF_STREAM);

        final PartitionSender pSender = ehClient.createPartitionSenderSync("0");
        pSender.send(new EventData(testMessage.getBytes()));

        final Iterable<EventData> events = pReceiver.receiveSync(100);
        Assert.assertEquals(testMessage, new String(events.iterator().next().getBytes()));

        pSender.closeSync();
        pReceiver.closeSync();;
        ehClient.closeSync();
    }

    // @Test
    public void runEventHubSendReceiveWithTokenProviderTest() throws Exception {

        final AuthenticationContext authenticationContext = new AuthenticationContext(
                "https://login.windows.net/" + TENANT_ID,
                true,
                executorService);
        final ClientCredential clientCredential = new ClientCredential(
                CLIENT_ID,
                CLIENT_SECRET);

        final String testMessage = "somedata test";

        final EventHubClient ehClient = EventHubClient.create(
                new URI("sb://" + NAMESPACE_ENDPOINT),
                EVENTHUB_NAME,
                new ITokenProvider() {
                    @Override
                    public CompletableFuture<SecurityToken> getToken(String resource, Duration timeout) {

                        final CompletableFuture<SecurityToken> result = new CompletableFuture<>();
                        authenticationContext.acquireToken(AzureActiveDirectoryTokenProvider.EVENTHUBS_REGISTERED_AUDIENCE,
                                clientCredential,
                                new AuthenticationCallback() {
                                    @Override
                                    public void onSuccess(AuthenticationResult authenticationResult) {
                                        result.complete(new SecurityToken(
                                                "jwt",
                                                authenticationResult.getAccessToken(),
                                                Date.from(Instant.now().plusSeconds(200))));
                                    }

                                    @Override
                                    public void onFailure(Throwable throwable) {
                                        result.completeExceptionally(throwable);
                                    }
                                });

                        return result;
                    }
                }).get();


        final PartitionReceiver pReceiver = ehClient.createReceiverSync("$Default", "0", PartitionReceiver.END_OF_STREAM);

        final PartitionSender pSender = ehClient.createPartitionSenderSync("0");
        pSender.send(new EventData(testMessage.getBytes()));

        final Iterable<EventData> events = pReceiver.receiveSync(100);
        Assert.assertEquals(testMessage, new String(events.iterator().next().getBytes()));

        pSender.closeSync();
        pReceiver.closeSync();;
        ehClient.closeSync();
    }

    // @Test(expected=AuthorizationFailedException.class)
    public void noRoleAssigned() throws Exception {

        // TODO: REMOVE ROLE ASSIGNMENT BEFORE RUNNING THE TEST (to be automated - manual step for now)
        final AuthenticationContext authenticationContext = new AuthenticationContext(
                "https://login.windows.net/" + TENANT_ID,
                true,
                executorService);
        final ClientCredential clientCredential = new ClientCredential(
                CLIENT_ID,
                CLIENT_SECRET);

        EventHubClient ehClient = EventHubClient.create(
                new URI("sb://" + NAMESPACE_ENDPOINT),
                EVENTHUB_NAME,
                authenticationContext,
                clientCredential).get();

        ehClient.sendSync(new EventData("some text".getBytes()));
    }

    // @Test
    public void performManualActionsRbacRoles() throws Exception {

        // TODO: this test keeps sending messages - perform actions on the background and see the expected output on console
        final AuthenticationContext authenticationContext = new AuthenticationContext(
                "https://login.windows.net/" + TENANT_ID,
                true,
                executorService);
        final ClientCredential clientCredential = new ClientCredential(
                CLIENT_ID,
                CLIENT_SECRET);

        EventHubClient ehClient = EventHubClient.create(
                new URI("sb://" + NAMESPACE_ENDPOINT),
                EVENTHUB_NAME,
                authenticationContext,
                clientCredential).get();

        while (true) {
            try {
                ehClient.sendSync(new EventData("some text".getBytes()));
                System.out.println(".");
            } catch (Exception exception) {
                System.out.println("Captured exception: ");
                exception.printStackTrace();
            }

            Thread.sleep(1000);
        }
    }

    @Test(expected=RuntimeException.class)
    public void invalidAuthenticationContextTest() throws Exception {

        final ConnectionStringBuilder connectionString = TestContext.getConnectionString();
        final ExecutorService exectorService = Executors.newCachedThreadPool();
        final AuthenticationContext authenticationContext = new AuthenticationContext(
                "https://login.windows.net/nonexistant",
                false,
                exectorService);

        final ClientCredential clientCredential = new ClientCredential("wrong_creds","random");
        final EventHubClient ehClient = EventHubClient.create(connectionString.getEndpoint(), connectionString.getEntityPath(), authenticationContext, clientCredential).get();
        ehClient.sendSync(new EventData("something".getBytes()));
    }

    @Test(expected=IllegalArgumentException.class)
    public void nullAuthenticationContextTest() throws Exception {

        final ConnectionStringBuilder connectionString = TestContext.getConnectionString();
        final ClientCredential clientCredential = new ClientCredential("wrong_creds","random");
        EventHubClient.create(
                connectionString.getEndpoint(),
                connectionString.getEntityPath(),
                null,
                clientCredential).get();
    }

    @Test(expected=IllegalArgumentException.class)
    public void nullClientCredsTest() throws Exception {

        final ConnectionStringBuilder connectionString = TestContext.getConnectionString();
        final ExecutorService exectorService = Executors.newCachedThreadPool();
        final AuthenticationContext authenticationContext = new AuthenticationContext(
                "https://login.windows.net/nonexistant",
                false,
                exectorService);

        EventHubClient.create(
                connectionString.getEndpoint(),
                connectionString.getEntityPath(),
                authenticationContext,
                (ClientCredential) null).get();
    }

    @Test(expected=ExecutionException.class)
    public void invalidNamespaceEndpointTest() throws Exception {

        final ConnectionStringBuilder connectionString = TestContext.getConnectionString();
        final ExecutorService exectorService = Executors.newCachedThreadPool();
        final AuthenticationContext authenticationContext = new AuthenticationContext(
                "https://login.windows.net/nonexistant",
                false,
                exectorService);
        final ClientCredential clientCredential = new ClientCredential("wrong_creds","random");

        EventHubClient.create(
                new URI("amqps://nonexistantNamespace"),
                connectionString.getEntityPath(),
                authenticationContext,
                clientCredential).get();
    }
}
