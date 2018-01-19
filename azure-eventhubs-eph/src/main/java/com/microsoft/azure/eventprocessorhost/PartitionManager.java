/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import com.microsoft.azure.eventhubs.IllegalEntityException;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionManager
{
	// Protected instead of private for testability
    protected final EventProcessorHost host;
    protected Pump pump;
    protected String partitionIds[] = null;
    
    final private Object scanFutureSynchronizer = new Object(); 
    private ScheduledFuture<?> scanFuture = null;

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(PartitionManager.class);

    PartitionManager(EventProcessorHost host)
    {
        this.host = host;
    }
    
    CompletableFuture<String[]> getPartitionIds()
    {
    	CompletableFuture<String[]> retval = null;
    	
    	if (this.partitionIds != null)
    	{
    		retval = CompletableFuture.completedFuture(this.partitionIds);
    	}
    	else
        {
    		// This try-catch is necessary because EventHubClient.createFromConnectionString can directly throw
    		// EventHubException or IOException, in addition to whatever failures may occur when the result of
    		// the CompletableFuture is evaluated.
    		try
    		{
    			// Stage 0: get EventHubClient for the event hub
				retval = EventHubClient.createFromConnectionString(this.host.getEventHubConnectionString())
				// Stage 1: use the client to get runtime info for the event hub 
				.thenComposeAsync((ehClient) -> ehClient.getRuntimeInformation(), this.host.getExecutorService())
				// Stage 2: extract the partition ids from the runtime info or throw on null (timeout)
				.thenAccept((EventHubRuntimeInformation ehInfo) ->
				{
					if (ehInfo != null)
					{
						this.partitionIds = ehInfo.getPartitionIds();

						TRACE_LOGGER.info(LoggingUtils.withHost(this.host,
				               "Eventhub " + this.host.getEventHubPath() + " count of partitions: " + ehInfo.getPartitionCount()));
						for (String id : this.partitionIds)
						{
							TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Found partition with id: " + id));
						}
					}
					else
					{
						throw new CompletionException(new TimeoutException("getRuntimeInformation returned null"));
					}
				})
				// Stage 3: RUN REGARDLESS OF EXCEPTIONS -- if there was an error, wrap it in IllegalEntityException and throw
				// Otherwise pass the partition ids along.
				.handleAsync((empty, e) ->
				{
					if (e != null)
					{
						Throwable notifyWith = e;
						if (e instanceof CompletionException)
						{
							notifyWith = e.getCause();
						}
						throw new CompletionException(new IllegalEntityException("Failure getting partition ids for event hub", notifyWith));
					}
					return this.partitionIds;
				});
			}
    		catch (EventHubException | IOException e)
    		{
    			retval = new CompletableFuture<String[]>();
    			retval.completeExceptionally(new IllegalEntityException("Failure getting partition ids for event hub", e));
			}
        }
        
        return retval;
    }

    // Testability hook: allows a test subclass to insert dummy pump.
    Pump createPumpTestHook()
    {
        return new Pump(this.host);
    }

    // Testability hook: called after stores are initialized.
    void onInitializeCompleteTestHook()
    {
    }

    // Testability hook: called at the end of the main loop after all partition checks/stealing is complete.
    void onPartitionCheckCompleteTestHook()
    {
    }
    
    CompletableFuture<Void> stopPartitions()
    {
    	// Stop the lease scanner.
    	synchronized (this.scanFutureSynchronizer)
    	{
    		this.scanFuture.cancel(false);
    	}

    	// Stop any partition pumps that are running.
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Shutting down all pumps"));
    	CompletableFuture<?>[] pumpRemovals = this.pump.removeAllPumps(CloseReason.Shutdown);
    	return CompletableFuture.allOf(pumpRemovals).whenCompleteAsync((empty, e) ->
    	{
    		if (e != null)
    		{
    			Throwable notifyWith = LoggingUtils.unwrapException(e, null);
    			TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, "Failure during shutdown"), notifyWith);
    			if (notifyWith instanceof Exception)
    			{
    				this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), (Exception) notifyWith,
    						EventProcessorHostActionStrings.PARTITION_MANAGER_CLEANUP);

    		        TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Partition manager exiting"));
    			}
    		}
    	}, this.host.getExecutorService());
    }
    
    public CompletableFuture<Void> initialize()
    {
    	this.pump = createPumpTestHook();
    	
    	// Stage 0: get partition ids and cache
    	return getPartitionIds()
    	// Stage 1: initialize stores, if stage 0 succeeded
    	.thenComposeAsync((unused) -> initializeStores(), this.host.getExecutorService())
    	// Stage 2: RUN REGARDLESS OF EXCEPTIONS -- trace errors
    	.whenCompleteAsync((empty, e) ->
    	{
    		if (e != null)
    		{
    			StringBuilder outAction = new StringBuilder();
    			Throwable notifyWith = LoggingUtils.unwrapException(e, outAction);
    			if (outAction.length() > 0)
    			{
    	    		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host,
    	                    "Exception while initializing stores (" + outAction.toString() + "), not starting partition manager"), notifyWith);
    			}
    			else
    			{
    	    		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host,
    	                    "Exception while initializing stores, not starting partition manager"), notifyWith);
    			}
    		}
    	}, this.host.getExecutorService())
    	// Stage 3: schedule scan, which will find partitions and start pumps, if previous stages succeeded
    	.thenRunAsync(() ->
    	{
			// Schedule the first scan right away.
    		synchronized (this.scanFutureSynchronizer)
    		{
    			this.scanFuture = this.host.getExecutorService().schedule(() -> scan(), 0, TimeUnit.SECONDS);
    		}
	    	
			onInitializeCompleteTestHook();
    	}, this.host.getExecutorService());
    }
    
    private CompletableFuture<?> initializeStores()
    {
        ILeaseManager leaseManager = this.host.getLeaseManager();
        ICheckpointManager checkpointManager = this.host.getCheckpointManager();
        
        // Stage 0: does lease store exist?
        CompletableFuture<?> initializeStoresFuture = leaseManager.leaseStoreExists()
        // Stage 1: create lease store if it doesn't exist
        .thenComposeAsync((exists) ->
        {
        	CompletableFuture<?> createLeaseStoreFuture = null;
        	if (!exists)
        	{
        		createLeaseStoreFuture = retryWrapper(() -> leaseManager.createLeaseStoreIfNotExists(), null,
        				"Failure creating lease store for this Event Hub, retrying",
						"Out of retries creating lease store for this Event Hub", EventProcessorHostActionStrings.CREATING_LEASE_STORE, 5);
        	}
        	else
        	{
        		createLeaseStoreFuture = CompletableFuture.completedFuture(null);
        	}
        	return createLeaseStoreFuture;
        }, this.host.getExecutorService())
        // Stage 2: does checkpoint store exist?
        .thenComposeAsync((unused) -> checkpointManager.checkpointStoreExists(), this.host.getExecutorService())
        // Stage 3: create checkpoint store if it doesn't exist
        .thenComposeAsync((exists) ->
        {
        	CompletableFuture<?> createCheckpointStoreFuture = null;
        	if (!exists)
        	{
        		createCheckpointStoreFuture = retryWrapper(() -> checkpointManager.createCheckpointStoreIfNotExists(), null,
        				"Failure creating checkpoint store for this Event Hub, retrying",
            			"Out of retries creating checkpoint store for this Event Hub", EventProcessorHostActionStrings.CREATING_CHECKPOINT_STORE, 5);
        	}
        	else
        	{
        		createCheckpointStoreFuture = CompletableFuture.completedFuture(null);
        	}
        	return createCheckpointStoreFuture;
        }, this.host.getExecutorService());
        
        // Stages 4-n: by now, either the stores exist (stages 0-3 succeeded) or one of them completed exceptionally and
        // all these stages will be skipped
        for (String id : this.partitionIds)
        {
        	final String iterationId = id;
        	initializeStoresFuture = initializeStoresFuture
        	// Stage X: create lease for partition <iterationId>
        	.thenComposeAsync((unused) ->
        	{
        		return retryWrapper(() -> leaseManager.createLeaseIfNotExists(iterationId), iterationId, "Failure creating lease for partition, retrying",
					"Out of retries creating lease for partition", EventProcessorHostActionStrings.CREATING_LEASE, 5);
        	}, this.host.getExecutorService())
        	// Stage X+1: create checkpoint holder for partition <iterationId>
        	.thenComposeAsync((unused) ->
        	{
        		return retryWrapper(() -> checkpointManager.createCheckpointIfNotExists(iterationId), iterationId, "Failure creating checkpoint for partition, retrying",
            			"Out of retries creating checkpoint blob for partition", EventProcessorHostActionStrings.CREATING_CHECKPOINT, 5);
        	}, this.host.getExecutorService());
        }

        return initializeStoresFuture;
    }
    
    // CompletableFuture will be completed exceptionally if it runs out of retries.
    private CompletableFuture<?> retryWrapper(Callable<CompletableFuture<?>> lambda, String partitionId, String retryMessage, String finalFailureMessage, String action, int maxRetries)
    {
    	CompletableFuture<?> retryResult = null;
    	
    	// Stage 0: first attempt
    	try
    	{
    		retryResult = lambda.call();
    	}
    	catch (Exception e)
    	{
    		retryResult = new CompletableFuture<Void>();
    		retryResult.completeExceptionally(e);
    	}
    	
    	for (int i = 1; i < maxRetries; i++)
    	{
    		retryResult = retryResult
    		// Stages 1, 3, 5, etc: trace errors but stop exception propagation in order to keep going
    		// Either return null if we don't have a valid result, or pass the result along to the next stage.
    		.handleAsync((r,e) ->
    		{
    			if (e != null)
    			{
        			if (partitionId != null)
        			{
        				TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, partitionId, retryMessage), LoggingUtils.unwrapException(e, null));
        			}
        			else
        			{
        				TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, retryMessage), LoggingUtils.unwrapException(e, null));
        			}
    			}
    			return (e == null) ? r : null; // stop propagation of exceptions
    		}, this.host.getExecutorService())
    		// Stages 2, 4, 6, etc: if we already have a valid result, pass it along. Otherwise, make another attempt.
    		// Once we have a valid result there will be no more exceptions.
    		.thenComposeAsync((oldresult) ->
    		{
    			CompletableFuture<?> newresult = CompletableFuture.completedFuture(oldresult);
    			if (oldresult == null)
    			{
	    			try
	    			{
						newresult = lambda.call();
					}
	    			catch (Exception e1)
	    			{
	    				throw new CompletionException(e1);
					}
    			}
    			return newresult;
    		}, this.host.getExecutorService());
    	}
    	// Stage final: trace the exception with the final message, or pass along the valid result.
    	retryResult = retryResult.handleAsync((r,e) ->
    	{
    		if (e != null)
    		{
        		if (partitionId != null)
        		{
        			TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, partitionId, finalFailureMessage));
        		}
        		else
        		{
        			TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, finalFailureMessage));
        		}
        		throw LoggingUtils.wrapException(new RuntimeException(finalFailureMessage), action);
    		}
    		return (e == null) ? r : null;
    	}, this.host.getExecutorService());
    	
    	return retryResult;
    }
    
    private class LeaseContext
    {
    	public LeaseContext(boolean should, Lease l) { this.shouldDoNextStep = should; this.lease = l; }
    	public boolean shouldDoNextStep;
    	public Lease lease;
    }
    
    private class BoolWrapper
    {
    	public BoolWrapper(boolean init) { this.value = init; }
    	public boolean value;
    }
    
    // Return Void so it can be called from a lambda.
    // throwOnFailure is true 
    private Void scan()
    {
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Starting lease scan"));

    	// DO NOT check whether this.scanFuture is cancelled. The first execution of this method is scheduled
    	// with 0 delay and can occur before this.scanFuture is set to the result of the schedule() call.

        ArrayList<CompletableFuture<Lease>> allLeasesResults = new ArrayList<CompletableFuture<Lease>>();
        // These are final so they can be used in the lambdas below.
        final AtomicInteger ourLeasesCount = new AtomicInteger();
        final ConcurrentHashMap<String, Lease> leasesOwnedByOthers = new ConcurrentHashMap<String, Lease>();
        final BoolWrapper resultsAreComplete = new BoolWrapper(true);

    	List<CompletableFuture<Lease>> gettingAllLeases = this.host.getLeaseManager().getAllLeases(this.partitionIds);

        for (CompletableFuture<Lease> leaseFuture : gettingAllLeases)
        {
        	// Stage 0: get the lease
        	CompletableFuture<Lease> oneResult = leaseFuture
        	// Stage 1: is the lease expired?
        	.thenComposeAsync((l) -> l.isExpired(), this.host.getExecutorService())
        	// Stage 2: combine the two previous results so stage 3 can consume both
        	.thenCombineAsync(leaseFuture, (exists, lease) ->
        	{
        		return new LeaseContext(exists, lease);
        	}, this.host.getExecutorService())
        	// Stage 3: if the lease is expired, acquire it and return success (true) or failure (false); always return false if not expired
        	.thenComposeAsync((lc) ->
        	{
        		return lc.shouldDoNextStep ? this.host.getLeaseManager().acquireLease(lc.lease) : CompletableFuture.completedFuture(false);
        	}, this.host.getExecutorService())
        	// Stage 4: if acquired, start a pump. Then do counting.
        	.thenCombineAsync(leaseFuture, (acquired, lease) ->
        	{
        		if (acquired)
        		{
        			this.pump.addPump(lease);
        		}
        		if (lease.isOwnedBy(this.host.getHostName()))
        		{
        			ourLeasesCount.getAndIncrement(); // count leases owned by this host
        		}
        		else
        		{
        			leasesOwnedByOthers.put(lease.getPartitionId(), lease); // save leases owned by other hosts
        		}
        		return lease;
        	}, this.host.getExecutorService())
        	// Stage 5: ALWAYS RUN REGARDLESS OF EXCEPTIONS -- log/notify if exception occurred
        	.whenCompleteAsync((lease, e) ->
        	{
        		if (e != null)
        		{
        			resultsAreComplete.value = false;
            		Exception notifyWith = (Exception)LoggingUtils.unwrapException(e, null);
            		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, "Failure getting/acquiring lease, skipping"), notifyWith);
            		this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith, EventProcessorHostActionStrings.CHECKING_LEASES,
            				ExceptionReceivedEventArgs.NO_ASSOCIATED_PARTITION);
        		}
        	}, this.host.getExecutorService());
        	allLeasesResults.add(oneResult);
        }

        // Stage A: complete when all per-lease results are in
        CompletableFuture<?>[] dummy = new CompletableFuture<?>[allLeasesResults.size()];
        CompletableFuture<Lease> leaseToStealFuture = CompletableFuture.allOf(allLeasesResults.toArray(dummy))
        // Stage B: consume the counting done by the per-lease stage to decide whether and what lease to steal
        .thenApplyAsync((empty) ->
        {
        	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Lease scan steal check"));
        	
            // Grab more leases if available and needed for load balancing, but only if all leases were checked OK.
            // Don't try to steal if numbers are in doubt due to errors in the previous stage. 
        	Lease stealThisLease = null;
            if ((leasesOwnedByOthers.size() > 0) && resultsAreComplete.value)
            {
	            stealThisLease = whichLeaseToSteal(leasesOwnedByOthers.values(), ourLeasesCount.get());
            }
        	return stealThisLease;
        }, this.host.getExecutorService());
        
        // Stage C: if B identified a candidate for stealing, attempt to steal it. Return true on successful stealing, false in all other cases
        leaseToStealFuture.thenComposeAsync((stealThisLease) ->
        {
        	return (stealThisLease != null) ? this.host.getLeaseManager().acquireLease(stealThisLease) : CompletableFuture.completedFuture(false);
        }, this.host.getExecutorService())
        // Stage D: consume results from B and C. Start a pump if a lease was stolen.
        .thenCombineAsync(leaseToStealFuture, (stealSucceeded, lease) ->
        {
            if (stealSucceeded)
            {
        		TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, lease, "Stole lease"));
        		this.pump.addPump(lease);
            }
            return lease;
        }, this.host.getExecutorService())
        // Stage E: ALWAYS RUN REGARDLESS OF EXCEPTIONS -- log/notify, schedule next scan
        .whenCompleteAsync((lease, e) ->
        {
        	if (e != null)
        	{
    			Exception notifyWith = (Exception)LoggingUtils.unwrapException(e, null);
    			if (lease != null)
    			{
	    			TRACE_LOGGER.warn(LoggingUtils.withHost(this.host,
	                        "Exception stealing lease for partition " + lease.getPartitionId()), notifyWith);
	    			this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith,
	    					EventProcessorHostActionStrings.STEALING_LEASE, lease.getPartitionId());
    			}
    			else
    			{
	    			TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, "Exception stealing lease"), notifyWith);
	    			this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith,
	    					EventProcessorHostActionStrings.STEALING_LEASE, ExceptionReceivedEventArgs.NO_ASSOCIATED_PARTITION);
    			}
        	}
        	
            onPartitionCheckCompleteTestHook();
            
        	// Schedule the next scan unless the future has been cancelled.
            synchronized (this.scanFutureSynchronizer)
            {
	        	if (!this.scanFuture.isCancelled())
	        	{
	        		int seconds = this.host.getPartitionManagerOptions().getLeaseRenewIntervalInSeconds();
	    	    	this.scanFuture = this.host.getExecutorService().schedule(() -> scan(), seconds, TimeUnit.SECONDS);
	    	    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Scheduling lease scanner in " + seconds));
	        	}
            }
        }, this.host.getExecutorService());

    	return null;
    }

    private Lease whichLeaseToSteal(Collection<Lease> stealableLeases, int haveLeaseCount)
    {
    	HashMap<String, Integer> countsByOwner = countLeasesByOwner(stealableLeases);
    	String biggestOwner = findBiggestOwner(countsByOwner);
    	int biggestCount = countsByOwner.get(biggestOwner); // HASHMAP
    	Lease stealThisLease = null;
    	
    	// If the number of leases is a multiple of the number of hosts, then the desired configuration is
    	// that all hosts own the name number of leases, and the difference between the "biggest" owner and
    	// any other is 0.
    	//
    	// If the number of leases is not a multiple of the number of hosts, then the most even configuration
    	// possible is for some hosts to have (leases/hosts) leases and others to have ((leases/hosts) + 1).
    	// For example, for 16 partitions distributed over five hosts, the distribution would be 4, 3, 3, 3, 3,
    	// or any of the possible reorderings.
    	//
    	// In either case, if the difference between this host and the biggest owner is 2 or more, then the
    	// system is not in the most evenly-distributed configuration, so steal one lease from the biggest.
    	// If there is a tie for biggest, findBiggestOwner() picks whichever appears first in the list because
    	// it doesn't really matter which "biggest" is trimmed down.
    	//
    	// Stealing one at a time prevents flapping because it reduces the difference between the biggest and
    	// this host by two at a time. If the starting difference is two or greater, then the difference cannot
    	// end up below 0. This host may become tied for biggest, but it cannot become larger than the host that
    	// it is stealing from.
    	
    	if ((biggestCount - haveLeaseCount) >= 2)
    	{
    		for (Lease l : stealableLeases)
    		{
    			if (l.isOwnedBy(biggestOwner))
    			{
    				stealThisLease = l;
    				TRACE_LOGGER.info(LoggingUtils.withHost(this.host,
                            "Proposed to steal lease for partition " + l.getPartitionId() + " from " + biggestOwner));
  					break;
    			}
    		}
    	}
    	return stealThisLease;
    }
    
    private String findBiggestOwner(HashMap<String, Integer> countsByOwner)
    {
    	int biggestCount = 0;
    	String biggestOwner = null;
    	for (String owner : countsByOwner.keySet())
    	{
    		if (countsByOwner.get(owner) > biggestCount) // HASHMAP
    		{
    			biggestCount = countsByOwner.get(owner); // HASHMAP
    			biggestOwner = owner;
    		}
    	}
    	return biggestOwner;
    }
    
    private HashMap<String, Integer> countLeasesByOwner(Iterable<Lease> leases)
    {
    	HashMap<String, Integer> counts = new HashMap<String, Integer>();
    	for (Lease l : leases)
    	{
    		if (counts.containsKey(l.getOwner()))
    		{
    			Integer oldCount = counts.get(l.getOwner()); // HASHMAP
    			counts.put(l.getOwner(), oldCount + 1);
    		}
    		else
    		{
    			counts.put(l.getOwner(), 1);
    		}
    	}
    	for (String owner : counts.keySet())
    	{
    		TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "host " + owner + " owns " + counts.get(owner) + " leases")); // HASHMAP
    	}
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "total hosts in sorted list: " + counts.size()));
    	
    	return counts;
    }
}
