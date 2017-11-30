package com.microsoft.azure.eventhubs.sendrecv;

import com.microsoft.aad.adal4j.AuthenticationCallback;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.azure.eventhubs.AzureActiveDirectoryTokenProvider;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.ClientConstants;
import com.microsoft.azure.eventhubs.ITokenProvider;
import com.microsoft.azure.eventhubs.SecurityToken;
import com.microsoft.azure.eventhubs.lib.ApiTestBase;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AdalTest extends ApiTestBase {

    // @Test
    public void runEventHubSendTest() throws Exception {

        final ExecutorService exectorService = Executors.newCachedThreadPool();
        final AuthenticationContext authenticationContext = new AuthenticationContext(
                "-------TenantAuthorityUrl---------",
                true,
                exectorService);
        final ClientCredential clientCredential = new ClientCredential(
                "----------ClientId------------",
                "-----------ClientSecret-----------");

        EventHubClient ehClient = EventHubClient.create(
                new URI("sb://namespace.servicebus.windows.net"),
                "----eventhubname-----",
                authenticationContext,
                clientCredential).get();

        ehClient.sendSync(new EventData("something test".getBytes()));
    }
}
