/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.sendrecv;

import java.time.Instant;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.ReceiverOptions;
import com.microsoft.azure.eventhubs.lib.ApiTestBase;
import com.microsoft.azure.eventhubs.lib.TestBase;
import com.microsoft.azure.eventhubs.lib.TestContext;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.ServiceBusException;

public class ReceiverRuntimeMetricsTest  extends ApiTestBase {
    
    static final String cgName = TestContext.getConsumerGroupName();
    static final String partitionId = "0";
    static final Instant beforeTestStart = Instant.now();

    static EventHubClient ehClient;

    static PartitionReceiver receiverWithOptions = null;
    static PartitionReceiver receiverWithoutOptions = null;
    static PartitionReceiver receiverWithOptionsDisabled = null;

    @BeforeClass
    public static void initializeEventHub()  throws Exception {
        
        final ConnectionStringBuilder connectionString = TestContext.getConnectionString();
        ehClient = EventHubClient.createFromConnectionStringSync(connectionString.toString());
        
        ReceiverOptions options = new ReceiverOptions();
        options.setReceiverRuntimeMetricEnabled(true);
        
        ReceiverOptions optionsWithMetricsDisabled = new ReceiverOptions();
        optionsWithMetricsDisabled.setReceiverRuntimeMetricEnabled(false);
        
        receiverWithOptions = ehClient.createReceiverSync(cgName, partitionId, Instant.now(), options);
        receiverWithoutOptions = ehClient.createReceiverSync(cgName, partitionId, Instant.EPOCH);
        receiverWithOptionsDisabled = ehClient.createReceiverSync(cgName, partitionId, Instant.EPOCH, optionsWithMetricsDisabled);
        
        TestBase.pushEventsToPartition(ehClient, partitionId, 25).get();
        
        receiverWithOptions.receiveSync(10);
        receiverWithoutOptions.receiveSync(10);
        receiverWithOptionsDisabled.receiveSync(10);
    }

    @Test()
    public void testRuntimeMetricsReturnedWhenEnabled() throws ServiceBusException {

        Assert.assertTrue(receiverWithOptions.getRuntimeInformation() != null);
        Assert.assertTrue(receiverWithOptions.getRuntimeInformation().getLastEnqueuedTime().isAfter(beforeTestStart));
    }

    @Test()
    public void testRuntimeMetricsWhenDisabled() throws ServiceBusException {

        Assert.assertTrue(receiverWithOptionsDisabled.getRuntimeInformation() == null);
    }
    
    @Test()
    public void testRuntimeMetricsDefaultDisabled() throws ServiceBusException {

        Assert.assertTrue(receiverWithoutOptions.getRuntimeInformation() == null);
    }
    
    @AfterClass()
    public static void cleanup() throws ServiceBusException {
        
        if (receiverWithOptions != null)
            receiverWithOptions.closeSync();
        
        if (receiverWithoutOptions != null)
            receiverWithoutOptions.closeSync();

        if (receiverWithOptionsDisabled != null)
            receiverWithOptionsDisabled.closeSync();
        
        if (ehClient != null)
            ehClient.closeSync();
    }
}
