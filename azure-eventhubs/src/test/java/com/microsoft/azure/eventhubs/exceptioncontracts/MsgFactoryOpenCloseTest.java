/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.exceptioncontracts;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.MessagingFactory;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.lib.ApiTestBase;
import com.microsoft.azure.eventhubs.lib.FaultInjectingReactorFactory;
import com.microsoft.azure.eventhubs.lib.TestContext;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class MsgFactoryOpenCloseTest extends ApiTestBase {

    static ConnectionStringBuilder connStr;

    @BeforeClass
    public static void initialize()  throws Exception
    {
        connStr = TestContext.getConnectionString();
    }

    @Test()
    public void VerifyThreadReleaseOnMsgFactoryOpenError() throws Exception    {

    }
}
