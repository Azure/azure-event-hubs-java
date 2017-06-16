/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

public class CheckpointAndLeaseManagerOptions
{
	public final static int DefaultLeaseDurationInSeconds = 45; // was 30
	public final static int DefaultLeaseRenewIntervalInSeconds = 10;
	
	private int leaseDurationInSeconds = CheckpointAndLeaseManagerOptions.DefaultLeaseDurationInSeconds;
	private int leaseRenewIntervalInSeconds = CheckpointAndLeaseManagerOptions.DefaultLeaseRenewIntervalInSeconds;
	
	public CheckpointAndLeaseManagerOptions()
	{
	}
	
	public int getLeaseDurationInSeconds() { return this.leaseDurationInSeconds; }
	
	public void setLeaseDurationInSeconds(int duration)
	{
		if (duration <= 0)
		{
			throw new IllegalArgumentException("Lease duration must be greater than 0 seconds");
		}
		this.leaseDurationInSeconds = duration;
	}
	
	public int getLeaseRenewIntervalInSeconds() { return this.leaseRenewIntervalInSeconds; }
	
	public void setLeaseRenewIntervalInSeconds(int interval)
	{
		if (interval <= 0)
		{
			throw new IllegalArgumentException("Lease renew interval must be greater than 0 seconds");
		}
		this.leaseRenewIntervalInSeconds = interval;
	}
}
