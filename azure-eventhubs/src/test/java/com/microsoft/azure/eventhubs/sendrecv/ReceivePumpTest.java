/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.sendrecv;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import com.microsoft.azure.eventhubs.TimeoutException;
import com.microsoft.azure.eventhubs.impl.IteratorUtil;
import com.microsoft.azure.eventhubs.impl.ReceivePump;
import com.microsoft.azure.eventhubs.lib.TestContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ReceivePumpTest {
    private final String exceptionMessage = "receive Exception";
    private volatile boolean assertion = false;

    @Before
    public void initializeValidation() {
        assertion = false;
    }

    @Test()
    public void testPumpOnReceiveEventFlow() {
        final ReceivePump receivePump = new ReceivePump(
                new ReceivePump.IPartitionReceiver() {
                    @Override
                    public CompletableFuture<Iterable<EventData>> receive(int maxBatchSize) {
                        final LinkedList<EventData> events = new LinkedList<EventData>();
                        events.add(EventData.create("some".getBytes()));
                        return CompletableFuture.completedFuture(events);
                    }

                    @Override
                    public String getPartitionId() {
                        return "0";
                    }
                },
                new PartitionReceiveHandler() {
                    @Override
                    public int getMaxEventCount() {
                        return 10;
                    }

                    @Override
                    public void onReceive(Iterable<EventData> events) {
                        assertion = IteratorUtil.sizeEquals(events, 1);

                        // stop-pump
                        throw new PumpClosedException();
                    }

                    @Override
                    public void onError(Throwable error) {
                        Assert.assertTrue(error instanceof PumpClosedException);
                    }
                },
                true,
                TestContext.EXECUTOR_SERVICE);

        receivePump.run();
        Assert.assertTrue(assertion);
    }

    @Test()
    public void testPumpReceiveTransientErrorsPropagated() throws EventHubException, InterruptedException, ExecutionException, TimeoutException {
        final ReceivePump receivePump = new ReceivePump(
                new ReceivePump.IPartitionReceiver() {
                    @Override
                    public CompletableFuture<Iterable<EventData>> receive(int maxBatchSize) {
                        final CompletableFuture<Iterable<EventData>> result = new CompletableFuture<>();
                        result.completeExceptionally(new RuntimeException(exceptionMessage));
                        return result;
                    }

                    @Override
                    public String getPartitionId() {
                        return "0";
                    }
                },
                new PartitionReceiveHandler() {
                    @Override
                    public int getMaxEventCount() {
                        return 10;
                    }

                    @Override
                    public void onReceive(Iterable<EventData> events) {
                    }

                    @Override
                    public void onError(Throwable error) {
                        assertion = error.getMessage().equals(exceptionMessage);
                    }
                },
                false,
                TestContext.EXECUTOR_SERVICE);

        receivePump.run();
        Assert.assertTrue(assertion);
    }

    @Test()
    public void testPumpReceiveExceptionsPropagated() throws EventHubException, InterruptedException, ExecutionException, TimeoutException {
        final ReceivePump receivePump = new ReceivePump(
                new ReceivePump.IPartitionReceiver() {
                    @Override
                    public CompletableFuture<Iterable<EventData>> receive(int maxBatchSize) {
                        final CompletableFuture<Iterable<EventData>> result = new CompletableFuture<>();
                        result.completeExceptionally(new RuntimeException(exceptionMessage));
                        return result;
                    }

                    @Override
                    public String getPartitionId() {
                        return "0";
                    }
                },
                new PartitionReceiveHandler() {
                    @Override
                    public int getMaxEventCount() {
                        return 10;
                    }

                    @Override
                    public void onReceive(Iterable<EventData> events) {
                    }

                    @Override
                    public void onError(Throwable error) {
                        assertion = error.getMessage().equals(exceptionMessage);
                    }
                },
                true,
                TestContext.EXECUTOR_SERVICE);

        receivePump.run();
        Assert.assertTrue(assertion);
    }

    @Test()
    public void testPumpOnReceiveExceptionsPropagated() throws EventHubException, InterruptedException, ExecutionException, TimeoutException {
        final String runtimeExceptionMsg = "random exception";
        final ReceivePump receivePump = new ReceivePump(
                new ReceivePump.IPartitionReceiver() {
                    @Override
                    public CompletableFuture<Iterable<EventData>> receive(int maxBatchSize) {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public String getPartitionId() {
                        return "0";
                    }
                },
                new PartitionReceiveHandler() {
                    @Override
                    public int getMaxEventCount() {
                        return 10;
                    }

                    @Override
                    public void onReceive(Iterable<EventData> events) {
                        throw new RuntimeException(runtimeExceptionMsg);
                    }

                    @Override
                    public void onError(Throwable error) {
                        assertion = error.getMessage().equals(runtimeExceptionMsg);
                    }
                },
                true,
                TestContext.EXECUTOR_SERVICE);

        receivePump.run();
        Assert.assertTrue(assertion);
    }

    public class PumpClosedException extends RuntimeException {
        private static final long serialVersionUID = -5050327636359966016L;
    }
}
