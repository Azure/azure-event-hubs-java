/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import java.time.Instant;

public interface EventHubRuntimeInformation {

    String getPath();

	Instant getCreatedAt();

    int getPartitionCount();

    String[] getPartitionIds();
}
