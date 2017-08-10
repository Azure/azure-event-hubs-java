/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.sendrecv;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.PartitionSender;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.PartitionReceiveHandler;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.microsoft.azure.eventhubs.lib.ApiTestBase;
import com.microsoft.azure.eventhubs.lib.TestContext;

public class EventDataBatchAPITest extends ApiTestBase {

    static final String cgName = TestContext.getConsumerGroupName();
    static final String partitionId = "0";
    static EventHubClient ehClient;
    static PartitionSender sender = null;

    @BeforeClass
    public static void initializeEventHub() throws Exception {
        final ConnectionStringBuilder connectionString = TestContext.getConnectionString();
        ehClient = EventHubClient.createFromConnectionStringSync(connectionString.toString());
        sender = ehClient.createPartitionSenderSync(partitionId);
    }

    @Test
    public void sendSmallEventsFullBatchTest()
            throws EventHubException, InterruptedException, ExecutionException, TimeoutException {
        final CompletableFuture<Void> validator = new CompletableFuture<>();
        final PartitionReceiver receiver = ehClient.createReceiverSync(cgName, partitionId, PartitionReceiver.END_OF_STREAM);
        receiver.setReceiveTimeout(Duration.ofSeconds(1));

        try {
            final EventDataBatch batchEvents = ehClient.createBatch();

            int count = 0;
            while (batchEvents.tryAdd(new EventData("a".getBytes())))
                count++;

            Assert.assertEquals(count, batchEvents.getSize());
            receiver.setReceiveHandler(new CountValidator(validator, count));

            sender = ehClient.createPartitionSenderSync(partitionId);
            sender.sendSync(batchEvents.toIterable());

            validator.get(30, TimeUnit.SECONDS);
            receiver.setReceiveHandler(null);
        }
        finally {
            receiver.closeSync();
        }
    }

    @Test
    public void sendEventsFullBatchWithAppPropsTest()
            throws EventHubException, InterruptedException, ExecutionException, TimeoutException {
        final CompletableFuture<Void> validator = new CompletableFuture<>();
        final PartitionReceiver receiver = ehClient.createReceiverSync(cgName, partitionId, PartitionReceiver.END_OF_STREAM);
        receiver.setReceiveTimeout(Duration.ofSeconds(1));

        try {
            final EventDataBatch batchEvents = sender.createBatch();

            int count = 0;
            while (true) {
                final EventData eventData = new EventData(new String(new char[new Random().nextInt(50000)]).replace("\0", "a").getBytes());
                for (int i = 0; i < new Random().nextInt(20); i++)
                    eventData.getProperties().put("somekey" + i, "somevalue");

                if (batchEvents.tryAdd(eventData))
                    count++;
                else
                    break;
            }

            Assert.assertEquals(count, batchEvents.getSize());
            receiver.setReceiveHandler(new CountValidator(validator, count));

            sender.sendSync(batchEvents.toIterable());

            validator.get(30, TimeUnit.SECONDS);

            receiver.setReceiveHandler(null);
        }finally {
            receiver.closeSync();
        }
    }

    @Test
    public void sendEventsFullBatchWithAllSectionsTest()
            throws EventHubException, InterruptedException, ExecutionException, TimeoutException {

        final EventDataBatch batchEvents = sender.createBatch();

        int count = 0;
        while (true) {
            final EventData eventData = new EventData(new String("a").getBytes());
            for (int i=0;i<new Random().nextInt(20);i++)
                eventData.getProperties().put("somekey" + i, "somevalue");

            if (batchEvents.tryAdd(eventData))
                count++;
            else
                break;
        }

        Assert.assertEquals(count, batchEvents.getSize());

        ehClient.sendSync(batchEvents.toIterable(), UUID.randomUUID().toString());
    }

    @AfterClass
    public static void cleanupClient() throws EventHubException
    {
        sender.closeSync();
        ehClient.closeSync();
    }

    public static class CountValidator extends PartitionReceiveHandler {
        final CompletableFuture<Void> validateSignal;
        final int netEventCount;

        int currentCount = 0;

        public CountValidator(final CompletableFuture<Void> validateSignal, final int netEventCount) {
            super(999);
            this.validateSignal = validateSignal;
            this.netEventCount = netEventCount;
        }

        @Override
        public void onReceive(Iterable<EventData> events) {
            if (events != null)
                for (EventData event : events) {
                    currentCount++;
                }

            if (currentCount >= netEventCount)
                this.validateSignal.complete(null);
        }

        @Override
        public void onError(Throwable error) {
            this.validateSignal.completeExceptionally(error);
        }
    }
}
