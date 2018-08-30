/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.exceptioncontracts;

import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.eventhubs.lib.ApiTestBase;
import com.microsoft.azure.eventhubs.lib.TestContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;

public class ClientEntityCreateTest extends ApiTestBase {
    static final String PARTITION_ID = "0";
    static ConnectionStringBuilder connStr;

    @BeforeClass
    public static void initialize() {
        connStr = TestContext.getConnectionString();
    }

    @Test()
    public void createReceiverShouldRetryAndThrowTimeoutExceptionUponRepeatedTransientErrors() throws Exception {
        setIsTransientOnIllegalEntityException(true);

        try {
            final ConnectionStringBuilder localConnStr = new ConnectionStringBuilder(connStr.toString());
            localConnStr.setOperationTimeout(Duration.ofSeconds(5)); // to retry atleast once

            final EventHubClient eventHubClient = EventHubClient.createSync(localConnStr.toString(), TestContext.EXECUTOR_SERVICE);

            try {
                eventHubClient.createReceiverSync("nonexistantcg", PARTITION_ID, EventPosition.fromStartOfStream());
                Assert.assertTrue(false); // this should be unreachable
            } catch (TimeoutException exception) {
                Assert.assertTrue(exception.getCause() instanceof IllegalEntityException);
            }

            eventHubClient.closeSync();
        } finally {
            setIsTransientOnIllegalEntityException(false);
        }
    }

    @Test()
    public void createSenderShouldRetryAndThrowTimeoutExceptionUponRepeatedTransientErrors() throws Exception {
        setIsTransientOnIllegalEntityException(true);

        try {
            final ConnectionStringBuilder localConnStr = new ConnectionStringBuilder(connStr.toString());
            localConnStr.setOperationTimeout(Duration.ofSeconds(5)); // to retry atleast once
            localConnStr.setEventHubName("nonexistanteventhub");
            final EventHubClient eventHubClient = EventHubClient.createSync(localConnStr.toString(), TestContext.EXECUTOR_SERVICE);

            try {
                eventHubClient.createPartitionSenderSync(PARTITION_ID);
                Assert.assertTrue(false); // this should be unreachable
            } catch (TimeoutException exception) {
                Assert.assertTrue(exception.getCause() instanceof IllegalEntityException);
            }

            eventHubClient.closeSync();
        } finally {
            setIsTransientOnIllegalEntityException(false);
        }
    }

    @Test()
    public void createInternalSenderShouldRetryAndThrowTimeoutExceptionUponRepeatedTransientErrors() throws Exception {
        setIsTransientOnIllegalEntityException(true);

        try {
            final ConnectionStringBuilder localConnStr = new ConnectionStringBuilder(connStr.toString());
            localConnStr.setOperationTimeout(Duration.ofSeconds(5)); // to retry atleast once
            localConnStr.setEventHubName("nonexistanteventhub");
            final EventHubClient eventHubClient = EventHubClient.createSync(localConnStr.toString(), TestContext.EXECUTOR_SERVICE);

            try {
                eventHubClient.sendSync(EventData.create("Testmessage".getBytes()));
                Assert.assertTrue(false); // this should be unreachable
            } catch (TimeoutException exception) {
                Assert.assertTrue(exception.getCause() instanceof IllegalEntityException);
            }

            eventHubClient.closeSync();
        } finally {
            setIsTransientOnIllegalEntityException(false);
        }
    }

    @Test(expected = IllegalEntityException.class)
    public void createReceiverShouldThrowRespectiveExceptionUponNonTransientErrors() throws Exception {
        setIsTransientOnIllegalEntityException(false);
        final ConnectionStringBuilder localConnStr = new ConnectionStringBuilder(connStr.toString());
        localConnStr.setOperationTimeout(Duration.ofSeconds(5)); // to retry atleast once

        final EventHubClient eventHubClient = EventHubClient.createSync(localConnStr.toString(), TestContext.EXECUTOR_SERVICE);

        try {
            eventHubClient.createReceiverSync("nonexistantcg", PARTITION_ID, EventPosition.fromStartOfStream());
        } finally {
            eventHubClient.closeSync();
        }
    }

    static void setIsTransientOnIllegalEntityException(final boolean value) throws Exception {
        final Field isTransientField = IllegalEntityException.class.getDeclaredField("IS_TRANSIENT");
        isTransientField.setAccessible(true);
        isTransientField.setBoolean(null, value);
    }
}
