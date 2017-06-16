/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

public class AzureStorageCheckpointAndLeaseManagerOptions extends CheckpointAndLeaseManagerOptions
{
	public final static int DefaultStorageMaximumExecutionTimeInSeconds = 30; // was 120
	
	private int storageMaximumExecutionTimeInSeconds = AzureStorageCheckpointAndLeaseManagerOptions.DefaultStorageMaximumExecutionTimeInSeconds;
	
	public AzureStorageCheckpointAndLeaseManagerOptions()
	{
	}
	
	public int getStorageMaximumExecutionTimeInSeconds() { return this.storageMaximumExecutionTimeInSeconds; }
	
	public void setStorageMaximumExecutionTimeInSeconds(int executionTime)
	{
		if (executionTime <= 0)
		{
			throw new IllegalArgumentException("Maximum execution time must be greater than 0 seconds");
		}
		
		this.storageMaximumExecutionTimeInSeconds = executionTime;
	}
}
