/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import com.microsoft.azure.eventhubs.EventHubException;

/**
 * Contract for all client entities with Open-Close/Abort state m/c
 * main-purpose: closeAll related entities
 * Internal-class
 */
abstract class ClientEntity extends ClosableBase {
    private final String clientId;

    protected ClientEntity(final String clientId, final ClientEntity parent, final Executor executor) {
    	super(parent, executor);
    	
        this.clientId = clientId;
    }

    public String getClientId() {
        return this.clientId;
    }
    
    public final CompletableFuture<Void> close() {
    	return close("clientId[" + this.clientId + "]");
    }
    
    public final void closeSync() throws EventHubException {
        try {
            this.close().get();
        } catch (InterruptedException | ExecutionException exception) {
            if (exception instanceof InterruptedException) {
                // Re-assert the thread's interrupted status
                Thread.currentThread().interrupt();
            }

            final Throwable throwable = exception.getCause();
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else if (throwable instanceof EventHubException) {
                throw (EventHubException) throwable;
            } else {
                throw new RuntimeException(throwable != null ? throwable : exception);
            }
        }
    }

    protected final void throwIfClosed() {
    	throwIfClosed(getLastKnownError());
    }

    protected Exception getLastKnownError() {
        return null;
    }
}
