/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

class NamedThreadFactory implements ThreadFactory {
	private final String poolName;
	private volatile int threadCount = 0;
	
	NamedThreadFactory(String poolName) {
		this.poolName = poolName;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread retval = Executors.defaultThreadFactory().newThread(r);
		retval.setName(poolName + "-" + this.threadCount++);
		return retval;
	}
}
