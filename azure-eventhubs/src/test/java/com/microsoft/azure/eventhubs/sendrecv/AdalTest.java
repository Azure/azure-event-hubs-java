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
        final ITokenProvider tokenProvider = new ITokenProvider() {
            @Override
            public CompletableFuture<SecurityToken> getToken(String resource, Duration tokenTimeToLive) {
                final CompletableFuture<SecurityToken> result = new CompletableFuture<>();
                authenticationContext.acquireToken(AzureActiveDirectoryTokenProvider.EVENTHUBS_REGISTERED_AUDIENCE,
                        clientCredential,
                        new AuthenticationCallback() {
                            @Override
                            public void onSuccess(AuthenticationResult authenticationResult) {

                                result.complete(new SecurityToken(ClientConstants.JWT_TOKEN_TYPE,
                                        authenticationResult.getAccessToken(),
                                        authenticationResult.getExpiresOnDate()));
                            }

                            @Override
                            public void onFailure(Throwable throwable) {
                                result.completeExceptionally(throwable);
                            }
                        });

                return result;
            }
        };

        EventHubClient ehClient = EventHubClient.create(
                new URI("sb://namespace.servicebus.windows.net"),
                "----eventhubname-----",
                tokenProvider).get();

        ehClient.sendSync(new EventData("something test".getBytes()));
    }
}
