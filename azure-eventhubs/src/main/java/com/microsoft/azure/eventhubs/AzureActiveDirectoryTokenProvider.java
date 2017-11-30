package com.microsoft.azure.eventhubs;

import com.microsoft.aad.adal4j.AuthenticationCallback;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public final class AzureActiveDirectoryTokenProvider implements ITokenProvider {

    public final static String EVENTHUBS_REGISTERED_AUDIENCE = "https://eventhubs.azure.net/";

    private final AuthenticationContext authenticationContext;
    private final ITokenAcquirer tokenAcquirer;

    public AzureActiveDirectoryTokenProvider(
            final AuthenticationContext authenticationContext,
            final ITokenAcquirer tokenAcquirer) {

        this.authenticationContext = authenticationContext;
        this.tokenAcquirer = tokenAcquirer;
    }

    @Override
    public CompletableFuture<SecurityToken> getToken(String resource, Duration tokenTimeToLive) {

        final CompletableFuture<SecurityToken> result = new CompletableFuture<>();
        this.tokenAcquirer.acquireToken(this.authenticationContext, new EventHubsAuthenticationCallback(result));
        return result;
    }

    public static class EventHubsAuthenticationCallback implements AuthenticationCallback {

        final CompletableFuture<SecurityToken> result;

        public EventHubsAuthenticationCallback(final CompletableFuture<SecurityToken> result) {

            this.result = result;
        }

        @Override
        public void onSuccess(AuthenticationResult authenticationResult) {

            this.result.complete(new SecurityToken(ClientConstants.JWT_TOKEN_TYPE,
                    authenticationResult.getAccessToken(),
                    authenticationResult.getExpiresOnDate()));
        }

        @Override
        public void onFailure(Throwable throwable) {

            this.result.completeExceptionally(throwable);
        }
    }

    public interface ITokenAcquirer {

        Future<AuthenticationResult> acquireToken(
                final AuthenticationContext authenticationContext,
                final AuthenticationCallback authenticationCallback);
    }
}
