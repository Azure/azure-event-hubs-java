/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Contract for entities with Open-Close/Abort state m/c
 * main-purpose: closeAll related entities
 * Internal-class
 */
public abstract class ClosableBase {

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(ClosableBase.class);
    protected final Executor executor;
    private final Object syncClose;
    private final ClosableBase parent;
    private CompletableFuture<Void> closeTask;
    private boolean isClosing;
    private boolean isClosed;

    protected ClosableBase(final ClosableBase parent, final Executor executor) {
        this.parent = parent;
        this.executor = executor;
        
        this.isClosing = false;
        this.isClosed = false;

        this.syncClose = new Object();
    }

    protected abstract CompletableFuture<Void> onClose();

    protected final boolean getIsClosed() {
        final boolean isParentClosed = this.parent != null && this.parent.getIsClosed();
        synchronized (this.syncClose) {
            return isParentClosed || this.isClosed;
        }
    }

    // returns true even if the Parent is (being) Closed
    protected final boolean getIsClosingOrClosed() {
        final boolean isParentClosingOrClosed = this.parent != null && this.parent.getIsClosingOrClosed();
        synchronized (this.syncClose) {
            return isParentClosingOrClosed || this.isClosing || this.isClosed;
        }
    }

    // Not every class can use the close/onClose model. Provide a way to manually set states.
    protected final void setClosing() {
    	synchronized (this.syncClose) {
    		this.isClosing = true;
    	}
    }
    
    // Not every class can use the close/onClose model. Provide a way to manually set states.
    protected final void setClosed() {
    	synchronized (this.syncClose) {
    		this.isClosing = false;
    		this.isClosed = true;
    	}
    }

    public CompletableFuture<Void> close(final String closeLogMessage) {
        synchronized (this.syncClose) {
            if (this.isClosed || this.isClosing) {
                return this.closeTask == null ? CompletableFuture.completedFuture(null) : this.closeTask;
            }

            this.isClosing = true;
        }

        if (TRACE_LOGGER.isInfoEnabled() && (closeLogMessage != null)) {
            TRACE_LOGGER.info("close: " + closeLogMessage);
        }

        this.closeTask = this.onClose().whenCompleteAsync((empty, e) -> {
                synchronized (ClosableBase.this.syncClose) {
                    ClosableBase.this.isClosing = false;
                    ClosableBase.this.isClosed = true;
                }
        }, this.executor);

        return this.closeTask;
    }
    
    protected final void throwIfClosed(final Exception lastError) {
    	throwIfClosed(String.format(Locale.US, "Operation not allowed after the %s instance is Closed.", this.getClass().getName()), lastError);
    }
    
    protected final void throwIfClosed(final String message) {
    	throwIfClosed(message, null);
    }

    protected final void throwIfClosed(final String message, final Exception lastError) {
        if (this.getIsClosingOrClosed()) {
            throw new IllegalStateException(message, lastError);
        }
    }
}
