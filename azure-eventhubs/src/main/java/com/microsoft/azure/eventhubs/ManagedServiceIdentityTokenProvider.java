/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

public class ManagedServiceIdentityTokenProvider implements ITokenProvider {

    static final String METADATA_HEADER_NAME = "Metadata";
    static final String LOCAL_REST_MSI_ENDPOINT_URL = "http://localhost:50342/oauth2/token";
    static final String MSI_URL_FORMAT = "%s?resource=%s";
    static final String MSI_URL = String.format(MSI_URL_FORMAT,
            LOCAL_REST_MSI_ENDPOINT_URL,
            AzureActiveDirectoryTokenProvider.EVENTHUBS_REGISTERED_AUDIENCE);

    @Override
    public CompletableFuture<SecurityToken> getToken(
            final String resource,
            final Duration timeout) {

        final CompletableFuture<SecurityToken> getTokenTask = new CompletableFuture<>();

        final URL msiUrl;
        try {
            msiUrl = new URL(MSI_URL);
        } catch (MalformedURLException malformedUrlException) {
            getTokenTask.completeExceptionally(malformedUrlException);
            return getTokenTask;
        }

        final HttpURLConnection httpConnection;
        try {
            httpConnection = (HttpURLConnection) msiUrl.openConnection();
        }catch (IOException ioException) {
            getTokenTask.completeExceptionally(ioException);
            return getTokenTask;
        }

        httpConnection.setRequestProperty(METADATA_HEADER_NAME, "true");

        try {
            httpConnection.setRequestMethod("GET");
        } catch(ProtocolException protocolException) {
            getTokenTask.completeExceptionally(protocolException);
            return getTokenTask;
        }

        httpConnection.setDoInput(true);

        try {
            httpConnection.connect();
        } catch (IOException ioException) {
            getTokenTask.completeExceptionally(ioException);
            return getTokenTask;
        }

        try {
            final StringBuilder responseBuilder = new StringBuilder();
            try (Reader reader = new InputStreamReader(httpConnection.getInputStream(), StandardCharsets.UTF_8)) {
                char[] buffer = new char[1024];
                int numBytesRead = -1;
                while ((numBytesRead = reader.read(buffer)) != -1) {
                    responseBuilder.append(buffer, 0, numBytesRead);
                }
            } catch (IOException ioException) {
                getTokenTask.completeExceptionally(ioException);
                return getTokenTask;
            }

            System.out.println("Response : " + responseBuilder.toString());

            final Gson gson = new Gson();
            final MSIToken token = gson.fromJson(responseBuilder.toString(), MSIToken.class);

            getTokenTask.complete(new SecurityToken("jwt", token.access_token, new Date(token.expires_on)));
            return getTokenTask;
        }finally {
            httpConnection.disconnect();
        }
    }

    private static class MSIToken
    {
        private String access_token;
        private String refresh_token;
        // Token validity in number of seconds
        private int expires_in;
        // Seconds from 1970-01-01T0:0:0Z UTC when the token will expire
        private long expires_on;
        // Seconds from 1970-01-01T0:0:0Z UTC after which the token takes effect
        private long not_before;
        // Resource for which token is requested
        private String resource;
        // Token type
        private String token_type;

        public String getAccessToken() {
            return access_token;
        }
        public int getExpiresIn() {
            return expires_in;
        }
        public long getExpiresOn() {
            return expires_on;
        }
        public long getNotBefore() {
            return not_before;
        }
        public String getResource() {
            return resource;
        }
        public String getTokenType() {
            return token_type;
        }
    }
}
