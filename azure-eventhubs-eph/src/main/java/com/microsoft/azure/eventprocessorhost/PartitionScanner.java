/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.eventprocessorhost;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.eventhubs.impl.ClosableBase;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

class PartitionScanner extends ClosableBase {
    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(PartitionScanner.class);
    private static final Random randomizer = new Random();
    private final HostContext hostContext;
    private final Consumer<Lease> addPump;
    
    // Populated by getAllLeaseStates()
    private List<LeaseStateInfo> allLeaseStates = null;
    
    // Values populated by sortLeasesAndCalculateDesiredCount
    private int desiredCount;
    private int unownedCount; // updated by acquireExpiredInChunksParallel
    final private ConcurrentHashMap<String, LeaseStateInfo> leasesOwnedByOthers; // updated by acquireExpiredInChunksParallel

    PartitionScanner(HostContext hostContext, Consumer<Lease> addPump, ClosableBase parent) {
    	super(parent, hostContext.getExecutor());
    	
        this.hostContext = hostContext;
        this.addPump = addPump;

        this.desiredCount = 0;
        this.unownedCount = 0;
        this.leasesOwnedByOthers = new ConcurrentHashMap<String, LeaseStateInfo>();
    }

    public CompletableFuture<Boolean> scan(boolean isFirst) {
        return getAllLeaseStates()
                .thenComposeAsync((unused) -> {
                	throwIfClosed("PartitionScanner is shutting down");
                    int ourLeasesCount = sortLeasesAndCalculateDesiredCount(isFirst);
                    return acquireExpiredInChunksParallel(0, this.desiredCount - ourLeasesCount);
                }, this.hostContext.getExecutor())
                .thenApplyAsync((remainingNeeded) -> {
                	throwIfClosed("PartitionScanner is shutting down");
                    ArrayList<LeaseStateInfo> stealThese = new ArrayList<LeaseStateInfo>();
                    if (remainingNeeded > 0) {
                        TRACE_LOGGER.debug(this.hostContext.withHost("Looking to steal: " + remainingNeeded));
                        stealThese = findLeasesToSteal(remainingNeeded);
                    }
                    return stealThese;
                }, this.hostContext.getExecutor())
                .thenComposeAsync((stealThese) -> {
                	throwIfClosed("PartitionScanner is shutting down");
                	return stealLeases(stealThese);
                }, this.hostContext.getExecutor())
                .handleAsync((didSteal, e) -> {
                    if ((e != null) && !getIsClosingOrClosed()) {
                        StringBuilder outAction = new StringBuilder();
                        Exception notifyWith = (Exception) LoggingUtils.unwrapException(e, outAction);
                        TRACE_LOGGER.warn(this.hostContext.withHost("Exception scanning leases"), notifyWith);
                        this.hostContext.getEventProcessorOptions().notifyOfException(this.hostContext.getHostName(), notifyWith, outAction.toString(),
                                ExceptionReceivedEventArgs.NO_ASSOCIATED_PARTITION);
                        didSteal = false;
                    }
                    return didSteal;
                }, this.hostContext.getExecutor());
    }

    private CompletableFuture<Void> getAllLeaseStates() {
    	throwIfClosed("PartitionScanner is shutting down");
        return this.hostContext.getLeaseManager().getAllLeasesStateInfo()
                .thenAcceptAsync((states) -> {
                	throwIfClosed("PartitionScanner is shutting down");
                    this.allLeaseStates = states;
                    Collections.sort(this.allLeaseStates);
                }, this.hostContext.getExecutor());
    }

    // NONBLOCKING
    private int sortLeasesAndCalculateDesiredCount(boolean isFirst) {
        TRACE_LOGGER.debug(this.hostContext.withHost("Accounting input: allLeaseStates size is " + this.allLeaseStates.size()));

        HashSet<String> uniqueOwners = new HashSet<String>();
        uniqueOwners.add(this.hostContext.getHostName());
        int ourLeasesCount = 0;
        this.unownedCount = 0;
        for (LeaseStateInfo info : this.allLeaseStates) {
            boolean ownedByUs = info.isOwned() && info.getOwner() != null && (info.getOwner().compareTo(this.hostContext.getHostName()) == 0);
            if (info.isOwned() && info.getOwner() != null) {
                uniqueOwners.add(info.getOwner());
            } else {
                this.unownedCount++;
            }
            if (ownedByUs) {
                ourLeasesCount++;
            } else if (info.isOwned()) {
                this.leasesOwnedByOthers.put(info.getPartitionId(), info);
            }
        }
        int hostCount = uniqueOwners.size();
        int countPerHost = this.allLeaseStates.size() / hostCount;
        this.desiredCount = isFirst ? 1 : countPerHost;
        if (!isFirst && (this.unownedCount > 0) && (this.unownedCount < hostCount) && ((this.allLeaseStates.size() % hostCount) != 0)) {
            // Distribute leftovers.
            this.desiredCount++;
        }

        ArrayList<String> sortedHosts = new ArrayList<String>(uniqueOwners);
        Collections.sort(sortedHosts);
        int hostOrdinal = -1;
        int startingPoint = 0;
        if (isFirst) {
            // If the entire system is starting up, the list of hosts is probably not complete and we can't really
            // compute a meaningful hostOrdinal. But we only want hostOrdinal to calculate startingPoint. Instead,
            // just randomly select a startingPoint.
            startingPoint = PartitionScanner.randomizer.nextInt(this.allLeaseStates.size());
        } else {
            for (hostOrdinal = 0; hostOrdinal < sortedHosts.size(); hostOrdinal++) {
                if (sortedHosts.get(hostOrdinal).compareTo(this.hostContext.getHostName()) == 0) {
                    break;
                }
            }
            startingPoint = countPerHost * hostOrdinal;
        }
        // Rotate allLeaseStates
        TRACE_LOGGER.debug(this.hostContext.withHost("Host ordinal: " + hostOrdinal + "  Rotating leases to start at " + startingPoint));
        if (startingPoint != 0) {
            ArrayList<LeaseStateInfo> rotatedList = new ArrayList<LeaseStateInfo>(this.allLeaseStates.size());
            for (int j = 0; j < this.allLeaseStates.size(); j++) {
                rotatedList.add(this.allLeaseStates.get((j + startingPoint) % this.allLeaseStates.size()));
            }
            this.allLeaseStates = rotatedList;
        }

        TRACE_LOGGER.debug(this.hostContext.withHost("Host count is " + hostCount + "  Desired owned count is " + this.desiredCount));
        TRACE_LOGGER.debug(this.hostContext.withHost("ourLeasesCount " + ourLeasesCount + "  leasesOwnedByOthers " + this.leasesOwnedByOthers.size()
                + " unowned " + unownedCount));

        return ourLeasesCount;
    }

    // NONBLOCKING
    // Returns a CompletableFuture as a convenience for the caller
    private CompletableFuture<List<LeaseStateInfo>> findExpiredLeases(int startAt, int endAt) {
        final ArrayList<LeaseStateInfo> expiredLeases = new ArrayList<LeaseStateInfo>();
        TRACE_LOGGER.debug(this.hostContext.withHost("Finding expired leases from '" + this.allLeaseStates.get(startAt).getPartitionId() + "'[" + startAt + "] up to '" +
                ((endAt < this.allLeaseStates.size()) ? this.allLeaseStates.get(endAt).getPartitionId() : "end") + "'[" + endAt + "]"));

        for (LeaseStateInfo info : this.allLeaseStates.subList(startAt, endAt)) {
            if (!info.isOwned()) {
                expiredLeases.add(info);
            }
        }

        TRACE_LOGGER.debug(this.hostContext.withHost("Found in range: " + expiredLeases.size()));
        return CompletableFuture.completedFuture(expiredLeases);
    }

    private CompletableFuture<Integer> acquireExpiredInChunksParallel(int startAt, int needed) {
    	throwIfClosed("PartitionScanner is shutting down");

        CompletableFuture<Integer> resultFuture = CompletableFuture.completedFuture(needed);
        if (startAt < this.allLeaseStates.size()) {
            TRACE_LOGGER.debug(this.hostContext.withHost("Examining chunk at '" + this.allLeaseStates.get(startAt).getPartitionId() + "'[" + startAt + "] need " + needed));
        } else {
            TRACE_LOGGER.debug(this.hostContext.withHost("Examining chunk skipping, startAt is off end: " + startAt));
        }

        if ((needed > 0) && (this.unownedCount > 0) && (startAt < this.allLeaseStates.size())) {
            final AtomicInteger runningNeeded = new AtomicInteger(needed);
            final int endAt = Math.min(startAt + needed, this.allLeaseStates.size());
            
            resultFuture = findExpiredLeases(startAt, endAt)
                    .thenComposeAsync((getThese) -> {
                    	throwIfClosed("PartitionScanner is shutting down");
                    	CompletableFuture<Void> acquireFuture = CompletableFuture.completedFuture(null);
                        if (getThese.size() > 0) {
                            ArrayList<CompletableFuture<Void>> getFutures = new ArrayList<CompletableFuture<Void>>();
                            for (LeaseStateInfo info : getThese) {
                            	throwIfClosed("PartitionScanner is shutting down");
                                final LeaseStateInfo workingInfo = info;
                                CompletableFuture<Void> getOneFuture = this.hostContext.getLeaseManager().getLease(workingInfo.getPartitionId())
                                        .thenComposeAsync((lease) -> {
                                        	throwIfClosed("PartitionScanner is shutting down");
                                            workingInfo.setLease(lease);
                                            return this.hostContext.getLeaseManager().acquireLease(lease);
                                        }, this.hostContext.getExecutor())
                                        .thenAcceptAsync((acquired) -> {
                                        	throwIfClosed("PartitionScanner is shutting down");
                                            if (acquired) {
                                                runningNeeded.decrementAndGet();
                                                TRACE_LOGGER.debug(this.hostContext.withHostAndPartition(workingInfo.getPartitionId(), "Acquired unowned/expired"));
                                                if (this.leasesOwnedByOthers.containsKey(workingInfo.getPartitionId())) {
                                                    this.leasesOwnedByOthers.remove(workingInfo.getPartitionId());
                                                    this.unownedCount--;
                                                }
                                                this.addPump.accept(workingInfo.getLease());
                                            } else {
                                                this.leasesOwnedByOthers.put(workingInfo.getPartitionId(), workingInfo);
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
                        if ((e != null) && !getIsClosingOrClosed()) {
                            Exception notifyWith = (Exception) LoggingUtils.unwrapException(e, null);
                            TRACE_LOGGER.warn(this.hostContext.withHost("Failure getting/acquiring lease, continuing"), notifyWith);
                            this.hostContext.getEventProcessorOptions().notifyOfException(this.hostContext.getHostName(), notifyWith,
                                    EventProcessorHostActionStrings.CHECKING_LEASES, ExceptionReceivedEventArgs.NO_ASSOCIATED_PARTITION);
                        }
                        return null;
                    }, this.hostContext.getExecutor())
                    .thenComposeAsync((unused) -> acquireExpiredInChunksParallel(endAt, runningNeeded.get()), this.hostContext.getExecutor());
        } else {
            TRACE_LOGGER.debug(this.hostContext.withHost("Short circuit: needed is 0, unowned is 0, or off end"));
        }

        return resultFuture;
    }

    // NONBLOCKING
    private ArrayList<LeaseStateInfo> findLeasesToSteal(int stealAsk) {
        // Generate a map of hostnames and owned counts.
        HashMap<String, Integer> hostOwns = new HashMap<String, Integer>();
        for (LeaseStateInfo info : this.leasesOwnedByOthers.values()) {
            if (hostOwns.containsKey(info.getOwner())) {
                int newCount = hostOwns.get(info.getOwner()) + 1;
                hostOwns.put(info.getOwner(), newCount);
            } else {
                hostOwns.put(info.getOwner(), 1);
            }
        }

        // Extract hosts which own more than the desired count
        ArrayList<String> bigOwners = new ArrayList<String>();
        for (Map.Entry<String, Integer> pair : hostOwns.entrySet()) {
            if (pair.getValue() > this.desiredCount) {
                bigOwners.add(pair.getKey());
                TRACE_LOGGER.debug(this.hostContext.withHost("Big owner " + pair.getKey() + " has " + pair.getValue()));
            }
        }

        ArrayList<LeaseStateInfo> stealInfos = new ArrayList<LeaseStateInfo>();

        if (bigOwners.size() > 0) {
            // Randomly pick one of the big owners
            String bigVictim = bigOwners.get(PartitionScanner.randomizer.nextInt(bigOwners.size()));
            int victimExtra = hostOwns.get(bigVictim) - this.desiredCount - 1;
            int stealCount = Math.min(victimExtra, stealAsk);
            TRACE_LOGGER.debug(this.hostContext.withHost("Stealing " + stealCount + " from " + bigVictim));

            // Grab stealCount partitions owned by bigVictim and return the infos.
            for (LeaseStateInfo candidate : this.allLeaseStates) {
                if (candidate.getOwner() != null && candidate.getOwner().compareTo(bigVictim) == 0) {
                    stealInfos.add(candidate);
                    if (stealInfos.size() >= stealCount) {
                        break;
                    }
                }
            }
        } else {
            TRACE_LOGGER.debug(this.hostContext.withHost("No big owners found, skipping steal"));
        }

        return stealInfos;
    }

    private CompletableFuture<Boolean> stealLeases(List<LeaseStateInfo> stealThese) {
        CompletableFuture<Boolean> allSteals = CompletableFuture.completedFuture(false);

        if (stealThese.size() > 0) {
            ArrayList<CompletableFuture<Void>> steals = new ArrayList<CompletableFuture<Void>>();
            for (LeaseStateInfo info : stealThese) {
            	throwIfClosed("PartitionScanner is shutting down");

                final LeaseStateInfo workingInfo = info;
                CompletableFuture<Void> oneSteal = this.hostContext.getLeaseManager().getLease(workingInfo.getPartitionId())
                        .thenComposeAsync((lease) -> {
                        	throwIfClosed("PartitionScanner is shutting down");
                            workingInfo.setLease(lease);
                            return this.hostContext.getLeaseManager().acquireLease(lease);
                        }, this.hostContext.getExecutor())
                        .thenAcceptAsync((acquired) -> {
                        	throwIfClosed("PartitionScanner is shutting down");
                            if (acquired) {
                                TRACE_LOGGER.debug(this.hostContext.withHostAndPartition(workingInfo.getPartitionId(), "Stole lease"));
                                this.addPump.accept(workingInfo.getLease());
                            }
                        }, this.hostContext.getExecutor());
                steals.add(oneSteal);
            }

            CompletableFuture<?> dummy[] = new CompletableFuture<?>[steals.size()];
            allSteals = CompletableFuture.allOf(steals.toArray(dummy)).thenApplyAsync((empty) -> true, this.hostContext.getExecutor());
        }

        return allSteals;
    }

	@Override
	protected CompletableFuture<Void> onClose() {
		// This class doesn't have a close. It uses the Closable functionality only so it can
		// bail out of a long-running scan rapidly on shutdown.
		return null;
	}
}
