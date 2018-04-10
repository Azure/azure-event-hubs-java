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
    private class LeaseAndState {
    	private Lease lease;
    	private boolean state;
    	public LeaseAndState(Lease l, boolean s) { this.lease = l; this.state = s; }
    	public Lease getLease() { return this.lease; }
    	public boolean isExpired() { return this.state; }
    }
    private ArrayList<LeaseAndState> allLeasesAndStates = null;
    
    // Values populated by initialCalcAndSort
    private int hostCount;
    private int lowestUnowned;
	final private AtomicInteger ourLeasesCount; // updated by acquireExpiredInChunksParallel
	final private ConcurrentHashMap<String, Lease> leasesOwnedByOthers; // updated by acquireExpiredInChunksParallel
	
	public PartitionScanner(HostContext hostContext, Consumer<Lease> addPump) {
		this.hostContext = hostContext;
		this.addPump = addPump;
		
		this.allLeasesAndStates = new ArrayList<LeaseAndState>();
		
		this.hostCount = 0;
		this.lowestUnowned = -1;
		this.ourLeasesCount = new AtomicInteger();
		this.leasesOwnedByOthers = new ConcurrentHashMap<String, Lease>();
	}
	
	public CompletableFuture<Lease> scan(boolean isFirst) {
		return getAllLeasesAndStates()
			.thenApplyAsync((empty) -> { return initialCalcAndSort(isFirst); }, this.hostContext.getExecutor())
			.thenComposeAsync((desiredLeaseCount) -> acquireExpiredInChunksParallel(this.lowestUnowned, desiredLeaseCount - ourLeasesCount.get()), this.hostContext.getExecutor())
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
	
	private CompletableFuture<Void> getAllLeasesAndStates() {
		return this.hostContext.getLeaseManager().getAllLeases().thenComposeAsync((leaseList) -> {
				ArrayList<CompletableFuture<Void>> statesFutures = new ArrayList<CompletableFuture<Void>>();
				for (Lease l : leaseList) {
					final Lease workingLease = l;
					statesFutures.add(workingLease.isExpired().thenAcceptAsync((isExpired) -> {
							synchronized (this.allLeasesAndStates) {
								this.allLeasesAndStates.add(new LeaseAndState(workingLease, isExpired));
							}
							//TRACE_LOGGER.info(this.hostContext.withHost("Lease for " + workingLease.getPartitionId() + " expired " + isExpired));
						}, this.hostContext.getExecutor()));
				}
		        CompletableFuture<?>[] dummy = new CompletableFuture<?>[statesFutures.size()];
				return CompletableFuture.allOf(statesFutures.toArray(dummy));
			}, this.hostContext.getExecutor());
	}
	
	private int initialCalcAndSort(boolean isFirst) {
		HashSet<String> uniqueOwners = new HashSet<String>();
		int addThisHost = 1;
		int unownedCount = 0;
		for (int i = 0; i < this.allLeasesAndStates.size(); i++) {
			//TRACE_LOGGER.info(this.hostContext.withHost("LeaseAndState for " + i + " is " + this.allLeasesAndStates.get(i)));
			boolean hasOwner = !this.allLeasesAndStates.get(i).isExpired();
			String owner = this.allLeasesAndStates.get(i).getLease().getOwner();
			boolean ownedByUs = hasOwner ? (owner.compareTo(this.hostContext.getHostName()) == 0) : false;
			if (hasOwner)
			{
				if (uniqueOwners.add(owner)) {
					// Haven't seen this owner before, is it us?
					if (ownedByUs) {
						// It is us. Don't add one to hostCount for us.
						addThisHost = 0;
					}
				}
			} else {
				unownedCount++;
			}
			if (ownedByUs) {
				this.ourLeasesCount.getAndIncrement();
			} else if (hasOwner) {
				this.leasesOwnedByOthers.put(this.allLeasesAndStates.get(i).getLease().getPartitionId(), this.allLeasesAndStates.get(i).getLease());
			} else if (this.lowestUnowned < 0) {
				this.lowestUnowned = i;
			}
		}
		if (this.lowestUnowned < 0) {
			this.lowestUnowned = this.allLeasesAndStates.size();
		}
		this.hostCount = uniqueOwners.size() + addThisHost;
		int desiredCount = isFirst ? 1 : (this.allLeasesAndStates.size() / this.hostCount);
		if (!isFirst && (unownedCount > 0) && (unownedCount < this.hostCount) && ((this.allLeasesAndStates.size() % this.hostCount) != 0)) {
			// Distribute leftovers.
			desiredCount++;
		}
		
		TRACE_LOGGER.info(this.hostContext.withHost("Host count is " + this.hostCount + "  Desired owned count is " + desiredCount));
		TRACE_LOGGER.info(this.hostContext.withHost("ourLeasesCount " + this.ourLeasesCount.get() + "  leasesOwnedByOthers " + this.leasesOwnedByOthers.size()
		 	+ " unowned " + unownedCount));
		
		return desiredCount;
	}
	
	private CompletableFuture<List<Lease>> findExpiredLeases(int startAt, int endAt) {
		final ArrayList<Lease> expiredLeases = new ArrayList<Lease>();
		TRACE_LOGGER.info(this.hostContext.withHost("Finding expired leases from " + startAt + " up to " + endAt));
		
		for (LeaseAndState ls : this.allLeasesAndStates.subList(startAt, endAt)) {
			if (ls.isExpired()) {
				expiredLeases.add(ls.getLease());
			}
		}

		return CompletableFuture.completedFuture(expiredLeases);
	}
	
	private CompletableFuture<Integer> acquireExpiredInChunksParallel(int startAt, int needed) {
		CompletableFuture<Integer> resultFuture = CompletableFuture.completedFuture(needed);
		TRACE_LOGGER.info(this.hostContext.withHost("Examining chunk at " + startAt + " need " + needed));
		
		if ((needed > 0) && (startAt < this.allLeasesAndStates.size())) {
			final AtomicInteger runningNeeded = new AtomicInteger(needed);
			int endAt = Math.min(startAt + needed, this.allLeasesAndStates.size());
			
			resultFuture = findExpiredLeases(startAt, endAt)
				.thenComposeAsync((getThese) -> {
						CompletableFuture<Void> acquireFuture = CompletableFuture.completedFuture(null);
						if (getThese.size() > 0) {
							ArrayList<CompletableFuture<Void>> getFutures = new ArrayList<CompletableFuture<Void>>();
							for (Lease l : getThese) {
								final Lease workingLease = l;
								if (workingLease != null) {
									CompletableFuture<Void> getOneFuture = this.hostContext.getLeaseManager().acquireLease(workingLease)
										.thenAcceptAsync((acquired) -> {
											if (acquired) {
												runningNeeded.decrementAndGet();
												TRACE_LOGGER.info(this.hostContext.withHostAndPartition(workingLease, "Acquired unowned/expired"));
												this.ourLeasesCount.incrementAndGet();
												this.addPump.accept(workingLease);
											} else {
												this.leasesOwnedByOthers.put(workingLease.getPartitionId(), workingLease);
											}
										}, this.hostContext.getExecutor());
									getFutures.add(getOneFuture);
								} else {
									TRACE_LOGGER.info(this.hostContext.withHost("findExpiredLeases returned null lease"));
								}
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
		} else {
			TRACE_LOGGER.info(this.hostContext.withHost("Short circuit: needed is 0 or off end"));
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
            TRACE_LOGGER.info(this.hostContext.withHost("host " + owner + " owns " + countsByOwner.get(owner) + " leases")); // FOO
        }
        TRACE_LOGGER.info(this.hostContext.withHost("total hosts in sorted list: " + countsByOwner.size())); // FOO

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
                    TRACE_LOGGER.info(this.hostContext.withHost("Proposed to steal lease for partition " + l.getPartitionId() + " from " + biggestOwner)); // FOO
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
	                        TRACE_LOGGER.info(this.hostContext.withHostAndPartition(leaseToSteal, "Stole lease")); // FOO
							this.addPump.accept(leaseToSteal);
						}
						return acquired ? leaseToSteal : null;
					}, this.hostContext.getExecutor());
		}
		
		return stealResult;
	}
}
