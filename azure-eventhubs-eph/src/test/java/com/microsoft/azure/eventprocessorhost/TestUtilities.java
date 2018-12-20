/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import org.junit.Assume;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

final class TestUtilities {
    static final ScheduledExecutorService EXECUTOR_SERVICE = new ScheduledThreadPoolExecutor(1);

    static void skipIfAppveyor() {
        String appveyor = System.getenv("APPVEYOR"); // Set to "true" by Appveyor
        if (appveyor != null) {
            TestBase.logInfo("SKIPPING - APPVEYOR DETECTED");
        }
        Assume.assumeTrue(appveyor == null);
    }

    static String getStorageConnectionString() {
        TestUtilities.skipIfAppveyor();

        String retval = System.getenv("EPHTESTSTORAGE");

        // if EPHTESTSTORAGE is not set - we cannot run integration tests
        if (retval == null) {
            TestBase.logInfo("SKIPPING - NO STORAGE CONNECTION STRING");
        }
        Assume.assumeTrue(retval != null);

        return ((retval != null) ? retval : "");
    }

    static Boolean isRunningOnAzure() {
        return (System.getenv("EVENT_HUB_CONNECTION_STRING") != null);
    }
}
