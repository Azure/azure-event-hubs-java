/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Centralize log message generation
 */
public final class LoggingUtils {
    static String withHost(String hostName, String logMessage) {
        return "host " + hostName + ": " + logMessage;
    }

    static String withHostAndPartition(String hostName, String partitionId, String logMessage) {
        return "host " + hostName + ": " + partitionId + ": " + logMessage;
    }

    static String withHostAndPartition(String hostName, PartitionContext context, String logMessage) {
        return "host " + hostName + ": " + context.getPartitionId() + ": " + logMessage;
    }
    
    // outAction can be null if you don't care about any action string
    static Exception unwrapException(Exception wrapped, StringBuilder outAction)
    {
    	Exception unwrapped = wrapped;
    	
    	while ((unwrapped instanceof ExecutionException) || (unwrapped instanceof CompletionException) ||
    			(unwrapped instanceof ExceptionWithAction))
    	{
    		if ((unwrapped instanceof ExceptionWithAction) && (outAction != null))
    		{
    			// Save the action string from an ExceptionWithAction, if desired.
    			outAction.append(((ExceptionWithAction)unwrapped).getAction());
    		}
    		
    		if ((unwrapped.getCause() != null) && (unwrapped.getCause() instanceof Exception))
    		{
    			unwrapped = (Exception)unwrapped.getCause();
    		}
			else
			{
				break;
			}
    	}

    	return unwrapped;
    }
    
    static String threadPoolStatusReport(String hostName, ScheduledExecutorService threadPool)
    {
    	String report = "";
    	
    	if (threadPool instanceof ThreadPoolExecutor)
    	{
    		ThreadPoolExecutor pool = (ThreadPoolExecutor)threadPool;
    		
            StringBuilder builder = new StringBuilder();
            builder.append("Thread pool settings: core: ");
            builder.append(pool.getCorePoolSize());
            builder.append("  active: ");
            builder.append(pool.getActiveCount());
            builder.append("  current: ");
            builder.append(pool.getPoolSize());
            builder.append("  largest: ");
            builder.append(pool.getLargestPoolSize());
            builder.append("  max: ");
            builder.append(pool.getMaximumPoolSize());
            builder.append("  policy: ");
            builder.append(pool.getRejectedExecutionHandler().getClass().toString());
            builder.append("  queue avail: ");
            builder.append(pool.getQueue().remainingCapacity());
            
            report = builder.toString();
    	}
    	else
    	{
    		report = "Cannot report on thread pool of type " + threadPool.getClass().toString();
    	}
    	
    	return report;
    }
}
