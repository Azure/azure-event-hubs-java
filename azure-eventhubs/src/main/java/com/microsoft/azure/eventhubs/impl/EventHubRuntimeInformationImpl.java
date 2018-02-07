/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.impl;

import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;

import java.time.Instant;

public final class EventHubRuntimeInformationImpl implements EventHubRuntimeInformation {

    final String path;
	final Instant createdAt;
    final int partitionCount;
    final String[] partitionIds;

    EventHubRuntimeInformationImpl(final String path, final Instant createdAt, final int partitionCount, final String[] partitionIds) {
        this.path = path;
        this.createdAt = createdAt;
        this.partitionCount = partitionCount;
        this.partitionIds = partitionIds;
    }

    public String getPath() {
        return this.path;
    }

	public Instant getCreatedAt() {
		return this.createdAt;
	}

    public int getPartitionCount() {
        return this.partitionCount;
    }

    public String[] getPartitionIds() {
        return this.partitionIds;
    }
}
