package com.microsoft.azure.eventhubs;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class AzureActiveDirectoryTokenProvider implements ITokenProvider {

    public final static String EVENTHUBS_REGISTERED_AUDIENCE = "https://eventhubs.azure.net/";

    @Override
    public CompletableFuture<SecurityToken> getToken(String resource, Duration tokenTimeToLive) {
        return null;
    }
}
