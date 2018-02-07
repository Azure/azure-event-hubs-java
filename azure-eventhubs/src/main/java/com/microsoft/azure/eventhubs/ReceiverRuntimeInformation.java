/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import java.time.Instant;

/**
 * Represents the temporal receiver runtime information for a {@link PartitionReceiver}.
 * Current received {@link EventData} and {@link ReceiverRuntimeInformation} can be used to find approximate value of pending events (which are not processed yet).
 */
public interface ReceiverRuntimeInformation {

    /**
     * Get PartitionId of the {@link PartitionReceiver} for which the {@link ReceiverRuntimeInformation} is returned.
     *
     * @return Partition Identifier
     */
    String getPartitionId();

    /**
     * Get sequence number of the {@link EventData}, that is written at the end of the Partition Stream.
     *
     * @return last sequence number
     */
    long getLastSequenceNumber();

    /**
     * Get enqueued time of the {@link EventData}, that is written at the end of the Partition Stream.
     *
     * @return last enqueued time
     */
    Instant getLastEnqueuedTime();

    /**
     * Get offset of the {@link EventData}, that is written at the end of the Partition Stream.
     *
     * @return last enqueued offset
     */
    String getLastEnqueuedOffset();

    /**
     * Get the timestamp at which this {@link ReceiverRuntimeInformation} was constructed.
     *
     * @return retrieval time
     */
    Instant getRetrievalTime();
}
