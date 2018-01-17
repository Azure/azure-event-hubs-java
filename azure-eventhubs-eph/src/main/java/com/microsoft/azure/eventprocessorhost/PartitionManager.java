/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
    		try
    		{
				retval = EventHubClient.createFromConnectionString(this.host.getEventHubConnectionString())
				.thenComposeAsync((ehClient) -> ehClient.getRuntimeInformation(), this.host.getExecutorService())
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
				.handleAsync((empty, e) ->
				{
					if (e != null)
					{
						throw new CompletionException(new IllegalEntityException("Failure getting partition ids for event hub", e));
					}
					return this.partitionIds;
				});
			}
    		catch (EventHubException | IOException e)
    		{
    			retval = new CompletableFuture<String[]>();
    			retval.completeExceptionally(new CompletionException(new IllegalEntityException("Failure getting partition ids for event hub", e)));
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
    	ScheduledFuture<?> captured = this.scanFuture;
    	if (captured != null)
    	{
    		captured.cancel(true);
    	}

    	// Stop any partition pumps that are running.
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Shutting down all pumps"));
    	CompletableFuture<Void>[] pumpRemovals = this.pump.removeAllPumps(CloseReason.Shutdown);
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
    	
    	return getPartitionIds()
    	.thenComposeAsync((unused) -> initializeStores(), this.host.getExecutorService()) 
    	.whenCompleteAsync((empty, e) ->
    	{
    		if (e != null)
    		{
    			StringBuilder outAction = new StringBuilder();
    			Throwable inner = LoggingUtils.unwrapException(e, outAction);
    			if (outAction.length() > 0)
    			{
    	    		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host,
    	                    "Exception while initializing stores (" + outAction.toString() + "), not starting partition manager"), inner);
    			}
    			else
    			{
    	    		TRACE_LOGGER.warn(LoggingUtils.withHost(this.host,
    	                    "Exception while initializing stores, not starting partition manager"), inner);
    			}
    			throw new CompletionException(inner);
    		}
    	}, this.host.getExecutorService())
    	.thenRunAsync(() ->
    	{
			// Schedule the first scan right away.
	    	this.scanFuture = this.host.getExecutorService().schedule(() -> scan(), 0, TimeUnit.SECONDS);
	    	
			onInitializeCompleteTestHook();
    	}, this.host.getExecutorService());
    }
    
    private CompletableFuture<Void> initializeStores()
    {
        ILeaseManager leaseManager = this.host.getLeaseManager();
        ICheckpointManager checkpointManager = this.host.getCheckpointManager();

        return leaseManager.leaseStoreExists().thenAcceptAsync((exists) ->
        {
        	// If the lease store doesn't exist, create it
        	if (!exists)
        	{
	        	try
	        	{
					retryWrapper(() -> leaseManager.createLeaseStoreIfNotExists(), null, "Failure creating lease store for this Event Hub, retrying",
							"Out of retries creating lease store for this Event Hub", EventProcessorHostActionStrings.CREATING_LEASE_STORE, 5);
				}
	        	catch (Exception e)
	        	{
	        		throw LoggingUtils.wrapException(e, EventProcessorHostActionStrings.CREATING_LEASE_STORE);
				}
        	}
        }, this.host.getExecutorService())
        .thenRunAsync(() ->
        {
        	// If we get here, the lease store exists. Now create the leases if necessary.
            for (String id : this.partitionIds)
            {
            	try
            	{
					retryWrapper(() -> leaseManager.createLeaseIfNotExists(id), id, "Failure creating lease for partition, retrying",
							"Out of retries creating lease for partition", EventProcessorHostActionStrings.CREATING_LEASE, 5);
				}
            	catch (Exception e)
            	{
            		throw LoggingUtils.wrapException(e, EventProcessorHostActionStrings.CREATING_LEASE);
				}
            }
        }, this.host.getExecutorService())
        .thenComposeAsync((empty) -> checkpointManager.checkpointStoreExists(), this.host.getExecutorService())
        .thenAcceptAsync((exists) ->
        {
        	// If the checkpoint store doesn't exist, create it
        	if (!exists)
            {
        		try
        		{
	            	retryWrapper(() -> checkpointManager.createCheckpointStoreIfNotExists(), null, "Failure creating checkpoint store for this Event Hub, retrying",
	            			"Out of retries creating checkpoint store for this Event Hub", EventProcessorHostActionStrings.CREATING_CHECKPOINT_STORE, 5);
				}
	        	catch (Exception e)
	        	{
	        		throw LoggingUtils.wrapException(e, EventProcessorHostActionStrings.CREATING_CHECKPOINT_STORE);
				}
            }
        	
        	// If we get here, the checkpoint store exists. Now create the checkpoint holders, if needed.
            for (String id : this.partitionIds)
            {
            	try
            	{
	            	retryWrapper(() -> checkpointManager.createCheckpointIfNotExists(id), id, "Failure creating checkpoint for partition, retrying",
	            			"Out of retries creating checkpoint blob for partition", EventProcessorHostActionStrings.CREATING_CHECKPOINT, 5);
				}
	        	catch (Exception e)
	        	{
	        		throw LoggingUtils.wrapException(e, EventProcessorHostActionStrings.CREATING_CHECKPOINT);
				}
            }
        }, this.host.getExecutorService());
    }
    
    // Throws if it runs out of retries. If it returns, action succeeded.
    private void retryWrapper(Callable<CompletableFuture<?>> lambda, String partitionId, String retryMessage, String finalFailureMessage, String action, int maxRetries) throws ExceptionWithAction
    {
    	boolean createdOK = false;
    	int retryCount = 0;
    	do
    	{
    		try
    		{
    			lambda.call().get(); // FIX
    			createdOK = true;
    		}
    		catch (Exception e)
    		{
    			if (partitionId != null)
    			{
    				TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, partitionId, retryMessage), e);
    			}
    			else
    			{
    				TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, retryMessage), e);
    			}
    			retryCount++;
    		}
    	} while (!createdOK && (retryCount < maxRetries));
    	if (!createdOK)
        {
    		if (partitionId != null)
    		{
    			TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, partitionId, finalFailureMessage));
    		}
    		else
    		{
    			TRACE_LOGGER.warn(LoggingUtils.withHost(this.host, finalFailureMessage));
    		}
    		throw new ExceptionWithAction(new RuntimeException(finalFailureMessage), action);
        }
    }
    
    private class LeaseContext
    {
    	public LeaseContext(boolean should, Lease l) { this.shouldDoNextStep = should; this.lease = l; }
    	public boolean shouldDoNextStep;
    	public Lease lease;
    }
    
    private class AllLeasesContext
    {
    	public boolean resultsAreComplete;
    	public int ourLeasesCount;
    	ArrayList<Lease> leasesOwnedByOthers;
    }
    
    // Return Void so it can be called from a lambda.
    // throwOnFailure is true 
    private Void scan()
    {
    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Starting lease scan"));

    	// DO NOT check whether this.scanFuture is cancelled. The first execution of this method is scheduled
    	// with 0 delay and can occur before this.scanFuture is set to the result of the schedule() call.

        List<CompletableFuture<Lease>> gettingAllLeases = this.host.getLeaseManager().getAllLeases(this.partitionIds);

        ArrayList<CompletableFuture<Lease>> allLeasesResults = new ArrayList<CompletableFuture<Lease>>();
        for (CompletableFuture<Lease> leaseFuture : gettingAllLeases)
        {
        	CompletableFuture<Lease> oneResult = leaseFuture
        	.thenComposeAsync((l) -> l.isExpired(), this.host.getExecutorService())
        	.thenCombineAsync(leaseFuture, (exists, lease) ->
        	{
        		return new LeaseContext(exists, lease);
        	}, this.host.getExecutorService())
        	.thenComposeAsync((lc) ->
        	{
        		return lc.shouldDoNextStep ? this.host.getLeaseManager().acquireLease(lc.lease) : CompletableFuture.completedFuture(false);
        	}, this.host.getExecutorService())
        	.thenCombineAsync(leaseFuture, (acquired, lease) ->
        	{
        		if (acquired)
        		{
        			this.pump.addPump(lease);
        		}
        		return lease;
        	}, this.host.getExecutorService());
        	allLeasesResults.add(oneResult);
        }

        CompletableFuture<Lease> leaseToStealFuture = CompletableFuture.allOf((CompletableFuture<?>[])allLeasesResults.toArray())
        .thenApplyAsync((empty) ->
        {
        	AllLeasesContext alc = new AllLeasesContext();
        	alc.resultsAreComplete = true;
        	alc.ourLeasesCount = 0;
        	alc.leasesOwnedByOthers = new ArrayList<Lease>();
        	
            for (CompletableFuture<Lease> leaseResult : allLeasesResults)
            {
            	Lease result = null;
            	try
            	{
	            	result = leaseResult.get(); // NON-BLOCKING, KNOWN TO BE COMPLETED DUE TO allOf()
	            	if (result.isOwnedBy(this.host.getHostName()))
	            	{
	            		alc.ourLeasesCount++;
	            	}
	            	else
	            	{
	            		alc.leasesOwnedByOthers.add(result);
	            	}
            	}
            	// Catch exceptions from processing of individual leases inside the loop so that all leases are touched.
            	catch (CompletionException|ExecutionException|InterruptedException e)
            	{
            		alc.resultsAreComplete = false;
            		Exception notifyWith = (Exception)LoggingUtils.unwrapException(e, null);
            		String partitionId = (result != null) ? result.getPartitionId() : ExceptionReceivedEventArgs.NO_ASSOCIATED_PARTITION;
            		TRACE_LOGGER.warn(LoggingUtils.withHostAndPartition(this.host, partitionId, "Failure getting/acquiring lease, skipping"),
            				notifyWith);
            		this.host.getEventProcessorOptions().notifyOfException(this.host.getHostName(), notifyWith, EventProcessorHostActionStrings.CHECKING_LEASES,
            				partitionId);
            	}
            }
            TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Our leases: " + alc.ourLeasesCount + "   other leases: " + alc.leasesOwnedByOthers.size()));
            return alc;
        }, this.host.getExecutorService())
        .thenApplyAsync((alc) ->
        {
            // Grab more leases if available and needed for load balancing, but only if all leases were checked OK.
            // Don't try to steal if numbers are in doubt due to errors in the previous stage. 
        	Lease stealThisLease = null;
            if ((alc.leasesOwnedByOthers.size() > 0) && alc.resultsAreComplete)
            {
	            stealThisLease = whichLeaseToSteal(alc.leasesOwnedByOthers, alc.ourLeasesCount);
            }
        	return stealThisLease;
        }, this.host.getExecutorService());
        
        
        leaseToStealFuture.thenComposeAsync((stealThisLease) ->
        {
        	return (stealThisLease != null) ? this.host.getLeaseManager().acquireLease(stealThisLease) : CompletableFuture.completedFuture(false);
        }, this.host.getExecutorService())
        .thenCombineAsync(leaseToStealFuture, (stealSucceeded, lease) ->
        {
            if (stealSucceeded)
            {
        		TRACE_LOGGER.info(LoggingUtils.withHostAndPartition(this.host, lease, "Stole lease"));
        		this.pump.addPump(lease);
            }
            return lease;
        }, this.host.getExecutorService())
        .whenCompleteAsync((lease, e) -> // always run the last stage regardless of exceptions
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
        	if (!this.scanFuture.isCancelled())
        	{
        		int seconds = this.host.getPartitionManagerOptions().getLeaseRenewIntervalInSeconds();
    	    	this.scanFuture = this.host.getExecutorService().schedule(() -> scan(), seconds, TimeUnit.SECONDS);
    	    	TRACE_LOGGER.info(LoggingUtils.withHost(this.host, "Scheduling lease scanner in " + seconds));
        	}
        }, this.host.getExecutorService());

    	return null;
    }

    private Lease whichLeaseToSteal(ArrayList<Lease> stealableLeases, int haveLeaseCount)
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
