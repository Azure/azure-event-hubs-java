/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import java.util.List;
import java.util.LinkedList;

/**
 * Helper class for creating a batch/collection of EventData objects to be used while Sending to EventHubs
 */
public final class EventDataBatch {

    private final static String MAX_PARTITION_KEY = new String(new char[ClientConstants.MAX_PARTITION_KEY_LENGTH]).replace("\0", "E");
    private final int maxMessageSize;
    private final List<EventData> events;
    private final byte[] eventBytes;
    private int currentSize = 0;

    EventDataBatch(final int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
        this.events = new LinkedList<>();
        this.currentSize = (maxMessageSize / 65536) * 1024; // reserve 1KB for every 64KB
        this.eventBytes = new byte[maxMessageSize];
    }

    /**
     * Get the number of events present in this {@link EventDataBatch}
     */
    public final int getSize() {
        return events.size();
    }

    /**
     * Add's {@link EventData} to {@link EventDataBatch}, if permitted by the batch's size limit.
     * This method is not thread-safe.
     *
     * @param eventData The {@link EventData} to add.
     * @return A boolean value indicating if the {@link EventData} addition to this batch/collection was successful or not.
     */
    public final boolean tryAdd(final EventData eventData) {

        if (eventData == null) {
            throw new IllegalArgumentException("eventData cannot be null");
        }

        final int size = getSize(eventData, events.isEmpty());
        if (this.currentSize + size > this.maxMessageSize)
            return false;

        this.events.add(eventData);
        this.currentSize += size;
        return true;
    }

    public final Iterable<EventData> toIterable() {
        return this.events;
    }

    private final int getSize(final EventData eventData, final boolean isFirst) {
        int eventSize = eventData.toAmqpMessage(MAX_PARTITION_KEY).encode(this.eventBytes, 0, maxMessageSize);
        eventSize += 16; // data section overhead
        if (isFirst) {
            eventSize += 512; // for headers
        }

        return eventSize;
    }
}
