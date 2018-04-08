/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionScanner {
    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(PartitionManager.class);
    private final HostContext hostContext;
    private final Consumer<Lease> addPump;
    
    // Populated by getAllLeases()
    private ArrayList<Lease> allLeases = null;
    
    // Values populated by initialCalcAndSort
    private int hostCount;
	final private AtomicInteger ourLeasesCount; // updated by acquireExpiredInChunksParallel
	final private ConcurrentHashMap<String, Lease> leasesOwnedByOthers; // updated by acquireExpiredInChunksParallel
	
	public PartitionScanner(HostContext hostContext, Consumer<Lease> addPump) {
		this.hostContext = hostContext;
		this.addPump = addPump;
		
		this.ourLeasesCount = new AtomicInteger();
		this.leasesOwnedByOthers = new ConcurrentHashMap<String, Lease>();
	}
	
	public CompletableFuture<Lease> scan(boolean isFirst) {
		return getAllLeases()
			.thenApplyAsync((empty) -> { return initialCalcAndSort(isFirst); }, this.hostContext.getExecutor())
			.thenComposeAsync((desiredLeaseCount) -> acquireExpiredInChunksParallel(0, desiredLeaseCount - ourLeasesCount.get()), this.hostContext.getExecutor())
			.thenApplyAsync((remainingNeeded) -> { return (remainingNeeded > 0) ? findLeaseToSteal() : null; }, this.hostContext.getExecutor())
			.thenComposeAsync((leaseToSteal) -> stealALease(leaseToSteal), this.hostContext.getExecutor())
	        .whenCompleteAsync((lease, e) -> {
		            if (e != null) {
		                Exception notifyWith = (Exception) LoggingUtils.unwrapException(e, null);
		                if (lease != null) {
		                    TRACE_LOGGER.warn(this.hostContext.withHost("Exception stealing lease for partition " + lease.getPartitionId()), notifyWith);
		                    this.hostContext.getEventProcessorOptions().notifyOfException(this.hostContext.getHostName(), notifyWith,
		                            EventProcessorHostActionStrings.STEALING_LEASE, lease.getPartitionId());
		                } else {
		                    TRACE_LOGGER.warn(this.hostContext.withHost("Exception stealing lease"), notifyWith);
		                    this.hostContext.getEventProcessorOptions().notifyOfException(this.hostContext.getHostName(), notifyWith,
		                            EventProcessorHostActionStrings.STEALING_LEASE, ExceptionReceivedEventArgs.NO_ASSOCIATED_PARTITION);
		                }
		            }
		        }, this.hostContext.getExecutor());
	}
	
	private CompletableFuture<Void> getAllLeases() {
		return this.hostContext.getLeaseManager().getAllLeases().thenAcceptAsync((leaseList) -> { this.allLeases = new ArrayList<Lease>(leaseList); });
	}
	
	private int initialCalcAndSort(boolean isFirst) {
		HashSet<String> uniqueOwners = new HashSet<String>();
		for (Lease l : this.allLeases) {
			String owner = l.getOwner();
			if ((owner != null) && !owner.isEmpty())
			{
				uniqueOwners.add(owner);
			}
			if (l.isOwnedBy(this.hostContext.getHostName())) {
				this.ourLeasesCount.getAndIncrement();
			} else {
				this.leasesOwnedByOthers.put(l.getPartitionId(), l);
			}
		}
		this.hostCount = uniqueOwners.size();
		return isFirst ? 1 : (this.allLeases.size() / this.hostCount);
	}
	
	private CompletableFuture<List<Lease>> findExpiredLeases(int startAt, int endAt) {
		ArrayList<Lease> expiredLeases = new ArrayList<Lease>();
		
		ArrayList<CompletableFuture<Void>> leaseStateFutures = new ArrayList<CompletableFuture<Void>>();
		for (Lease l : this.allLeases.subList(startAt, endAt)) {
			final Lease workingLease = l;
			CompletableFuture<Void> isExpiredFuture = workingLease.isExpired()
    			.thenAcceptAsync((isExpired) -> {
    				if (isExpired) {
    					expiredLeases.add(workingLease);
    				}
    			}, this.hostContext.getExecutor());
			leaseStateFutures.add(isExpiredFuture);
		}

        CompletableFuture<?>[] dummy = new CompletableFuture<?>[leaseStateFutures.size()];
        return CompletableFuture.allOf(leaseStateFutures.toArray(dummy)).thenApplyAsync((empty) -> { return expiredLeases; }, this.hostContext.getExecutor());
	}
	
	private CompletableFuture<Integer> acquireExpiredInChunksParallel(int startAt, int needed) {
		CompletableFuture<Integer> resultFuture = CompletableFuture.completedFuture(needed);
		
		if ((needed > 0) && (startAt < this.allLeases.size())) {
			final AtomicInteger runningNeeded = new AtomicInteger(needed);
			int endAt = Math.min(startAt + needed, this.allLeases.size());
			
			resultFuture = findExpiredLeases(startAt, endAt)
				.thenComposeAsync((getThese) -> {
						CompletableFuture<Void> acquireFuture = CompletableFuture.completedFuture(null);
						if (getThese.size() > 0) {
							ArrayList<CompletableFuture<Void>> getFutures = new ArrayList<CompletableFuture<Void>>();
							for (Lease l : getThese) {
								final Lease workingLease = l;
								CompletableFuture<Void> getOneFuture = this.hostContext.getLeaseManager().acquireLease(workingLease)
									.thenAcceptAsync((acquired) -> {
										if (acquired) {
											runningNeeded.decrementAndGet();
											this.ourLeasesCount.incrementAndGet();
											this.addPump.accept(workingLease);
										} else {
											this.leasesOwnedByOthers.put(workingLease.getPartitionId(), workingLease);
										}
									}, this.hostContext.getExecutor());
								getFutures.add(getOneFuture);
							}
							CompletableFuture<?>[] dummy = new CompletableFuture<?>[getFutures.size()];
							acquireFuture = CompletableFuture.allOf(getFutures.toArray(dummy));
						}
						return acquireFuture;
					}, this.hostContext.getExecutor())
				.handleAsync((empty, e) -> {
						// log/notify if exception occurred, then swallow exception and continue with next chunk
						if (e != null) {
                            Exception notifyWith = (Exception)LoggingUtils.unwrapException(e, null);
                            TRACE_LOGGER.warn(this.hostContext.withHost("Failure getting/acquiring lease, skipping"), notifyWith);
                            this.hostContext.getEventProcessorOptions().notifyOfException(this.hostContext.getHostName(), notifyWith,
                                    EventProcessorHostActionStrings.CHECKING_LEASES, ExceptionReceivedEventArgs.NO_ASSOCIATED_PARTITION);
						}
						return null;
					}, this.hostContext.getExecutor())
				.thenComposeAsync((unused) -> {
					return acquireExpiredInChunksParallel(endAt, runningNeeded.get());
					}, this.hostContext.getExecutor());
		}
		
		return resultFuture;
	}
	
	private Lease findLeaseToSteal()
	{
        HashMap<String, Integer> countsByOwner = new HashMap<String, Integer>();
        for (Lease l : this.leasesOwnedByOthers.values()) {
            if (countsByOwner.containsKey(l.getOwner())) {
                Integer oldCount = countsByOwner.get(l.getOwner());
                countsByOwner.put(l.getOwner(), oldCount + 1);
            } else {
                countsByOwner.put(l.getOwner(), 1);
            }
        }
        for (String owner : countsByOwner.keySet()) {
            TRACE_LOGGER.debug(this.hostContext.withHost("host " + owner + " owns " + countsByOwner.get(owner) + " leases")); // HASHMAP
        }
        TRACE_LOGGER.debug(this.hostContext.withHost("total hosts in sorted list: " + countsByOwner.size()));

        int biggestCount = 0;
        String biggestOwner = null;
        for (String owner : countsByOwner.keySet()) {
            if (countsByOwner.get(owner) > biggestCount) {
                biggestCount = countsByOwner.get(owner);
                biggestOwner = owner;
            }
        }

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

        Lease stealThisLease = null;
        if ((biggestCount - this.ourLeasesCount.get()) >= 2) {
            for (Lease l : this.leasesOwnedByOthers.values()) {
                if (l.isOwnedBy(biggestOwner)) {
                    stealThisLease = l;
                    TRACE_LOGGER.debug(this.hostContext.withHost("Proposed to steal lease for partition " + l.getPartitionId() + " from " + biggestOwner));
                    break;
                }
            }
        }
        return stealThisLease;
	}
	
	private CompletableFuture<Lease> stealALease(Lease leaseToSteal)
	{
		CompletableFuture<Lease> stealResult = CompletableFuture.completedFuture(null);
		
		if (leaseToSteal != null) {
			stealResult = this.hostContext.getLeaseManager().acquireLease(leaseToSteal)
				.thenApplyAsync((acquired) -> {
						if (acquired) {
	                        TRACE_LOGGER.debug(this.hostContext.withHostAndPartition(leaseToSteal, "Stole lease"));
							this.addPump.accept(leaseToSteal);
						}
						return acquired ? leaseToSteal : null;
					}, this.hostContext.getExecutor());
		}
		
		return stealResult;
	}
}
