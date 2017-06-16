/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

public class AzureStorageCheckpointAndLeaseManagerOptions extends CheckpointAndLeaseManagerOptions
{
	public AzureStorageCheckpointAndLeaseManagerOptions()
	{
	}

	@Override
	public void setLeaseDurationInSeconds(int duration)
	{
		// Max Azure Storage blob lease is 60 seconds
		if ((duration <= 0) || (duration > 60))
		{
			throw new IllegalArgumentException("Lease duration must be greater than 0 and not more than 60 seconds");
		}
		this.leaseDurationInSeconds = duration;
	}
}
