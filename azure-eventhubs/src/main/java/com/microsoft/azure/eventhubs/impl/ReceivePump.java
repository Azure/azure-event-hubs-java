/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.impl;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

public class ReceivePump {
    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(ReceivePump.class);

    private final IPartitionReceiver receiver;
    private final PartitionReceiveHandler onReceiveHandler;
    private final boolean invokeOnTimeout;
    private final CompletableFuture<Void> stopPump;
    private final Executor executor;

    private AtomicBoolean stopPumpRaised;
    private volatile boolean isPumpHealthy = true;

    public ReceivePump(
            final IPartitionReceiver receiver,
            final PartitionReceiveHandler receiveHandler,
            final boolean invokeOnReceiveWithNoEvents,
            final Executor executor) {
        this.receiver = receiver;
        this.onReceiveHandler = receiveHandler;
        this.invokeOnTimeout = invokeOnReceiveWithNoEvents;
        this.stopPump = new CompletableFuture<Void>();
        this.executor = executor;

        this.stopPumpRaised = new AtomicBoolean(false);
    }

    public void start() {
        try {
            ReceivePump.this.run();
        } catch (final Exception exception) {
            if (TRACE_LOGGER.isErrorEnabled()) {
                TRACE_LOGGER.error(
                        String.format("Receive pump for partition (%s) encountered unrecoverable error and exited with exception %s.",
                                ReceivePump.this.receiver.getPartitionId(), exception.toString()));
            }

            throw exception;
        }
    }

    public void run() {
        if (this.shouldContinue()) {
            this.receiver.receive(this.onReceiveHandler.getMaxEventCount())
                .handleAsync(
                    new BiFunction<Iterable<EventData>, Throwable, Void>() {
                        @Override
                        public Void apply(final Iterable<EventData> receivedEvents, final Throwable clientException) {

                            if (clientException != null) {
                                ReceivePump.this.isPumpHealthy = false;

                                if (TRACE_LOGGER.isWarnEnabled()) {
                                    TRACE_LOGGER.warn(String.format(
                                            "Receive pump for partition (%s) exiting after receive exception %s",
                                            ReceivePump.this.receiver.getPartitionId(), clientException.toString()));
                                }

                                ReceivePump.this.onReceiveHandler.onError(clientException);
                            }

                            try {
                                // don't invoke user call back - if stop is already raised / pump is unhealthy
                                if (ReceivePump.this.shouldContinue() &&
                                        (receivedEvents != null
                                        || (receivedEvents == null && ReceivePump.this.invokeOnTimeout))) {
                                    ReceivePump.this.onReceiveHandler.onReceive(receivedEvents);
                                }
                            } catch (final Throwable userCodeError) {
                                ReceivePump.this.isPumpHealthy = false;
                                if (TRACE_LOGGER.isErrorEnabled()) {
                                    TRACE_LOGGER.error(
                                            String.format("Receive pump for partition (%s) exiting after user-code exception %s",
                                                    ReceivePump.this.receiver.getPartitionId(), userCodeError.toString()));
                                }

                                ReceivePump.this.onReceiveHandler.onError(userCodeError);

                                if (userCodeError instanceof InterruptedException) {
                                    if (TRACE_LOGGER.isInfoEnabled()) {
                                        TRACE_LOGGER.info(String.format("Interrupting receive pump for partition (%s)",
                                                ReceivePump.this.receiver.getPartitionId()));
                                    }

                                    Thread.currentThread().interrupt();
                                }
                            }

                            return null;
                        }
                    }, this.executor);

            try {
                this.executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        ReceivePump.this.start();
                    }
                });
            } catch (final RejectedExecutionException rejectedException) {
                this.isPumpHealthy = false;

                if (TRACE_LOGGER.isWarnEnabled()) {
                    TRACE_LOGGER.warn(String.format(
                            "Receive pump for partition (%s) exiting with error: %s",
                            this.receiver.getPartitionId(), rejectedException.toString()));
                }

                this.onReceiveHandler.onError(rejectedException);
            }
        } else {
            if (TRACE_LOGGER.isInfoEnabled()) {
                TRACE_LOGGER.info(String.format("Stopping receive pump for partition (%s) as %s",
                        ReceivePump.this.receiver.getPartitionId(),
                        this.stopPumpRaised.get() ? "per the request." : "pump ran into errors."));
            }

            this.stopPump.complete(null);
        }
    }

    public CompletableFuture<Void> stop() {
        this.stopPumpRaised.set(true);
        return this.stopPump;
    }

    public boolean isRunning() {
        return !this.stopPump.isDone();
    }

    // partition receiver contract against which this pump works
    public interface IPartitionReceiver {
        String getPartitionId();

        CompletableFuture<Iterable<EventData>> receive(final int maxBatchSize);
    }

    private boolean shouldContinue() {
        return this.isPumpHealthy && !this.stopPumpRaised.get();
    }
}
