/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import java.time.Instant;

/**
 * Defines a position of an {@link EventData} in the event hub partition.
 * The position can be an Offset, Sequence Number, or EnqueuedTime.
 */
public class EventPosition {

    private final String offset;
    private final Long sequenceNumber;
    private final Instant dateTime;
    private final Boolean inclusive;

    private EventPosition(String o, Long s, Instant d, Boolean i) {
        this.offset = o;
        this.sequenceNumber = s;
        this.dateTime = d;
        this.inclusive = i;
    }

    /**
     * Creates a position at the given offset. The specified event will not be included.
     * Instead, the next event is returned.
     * @param offset    is the byte offset of the event.
     * @return An {@link EventPosition} object.
     */
    public static EventPosition fromOffset(String offset) {
        return EventPosition.fromOffset(offset, false);
    }

    /**
     * Creates a position at the given offset.
     * @param offset    is the byte offset of the event.
     * @param inclusive will include the specified event when set to true; otherwise, the next event is returned.
     * @return An {@link EventPosition} object.
     */
    public static EventPosition fromOffset(String offset, boolean inclusive) {
        return new EventPosition(offset, null, null, inclusive);
    }

    /**
     * Creates a position at the given sequence number. The specified event will not be included.
     * Instead, the next event is returned.
     * @param sequenceNumber is the sequence number of the event.
     * @return An {@link EventPosition} object.
     */
    public static EventPosition fromSequenceNumber(Long sequenceNumber) {
        return EventPosition.fromSequenceNumber(sequenceNumber, false);
    }

    /**
     * Creates a position at the given sequence number. The specified event will not be included.
     * Instead, the next event is returned.
     * @param sequenceNumber    is the sequence number of the event.
     * @param inclusive         will include the specified event when set to true; otherwise, the next event is returned.
     * @return An {@link EventPosition} object.
     */
    public static EventPosition fromSequenceNumber(Long sequenceNumber, boolean inclusive) {
        return new EventPosition(null, sequenceNumber, null, inclusive);
    }

    /**
     * Creates a position at the given {@link Instant}.
     * @param dateTime  is the enqueued time of the event.
     * @return An {@link EventPosition} object.
     */
    public static EventPosition fromEnqueuedTime(Instant dateTime) {
        return new EventPosition(null, null, dateTime, null);
    }

    /**
     * @return An {@link EventPosition} with position set to the start of the Event Hubs stream.
     */
    public static EventPosition fromStartOfStream() {
        return new EventPosition(PartitionReceiver.START_OF_STREAM, null, null, false);
    }

    /**
     * @param inclusive When true, the last event in the partition will be sent. When false,
     *                  the last event will be skipped, and only new events will be received.
     * @return An {@link EventPosition} with position set to the end of the Event Hubs stream.
     */
    public static EventPosition fromEndOfStream(boolean inclusive) {
        return new EventPosition(PartitionReceiver.END_OF_STREAM, null, null, inclusive);
    }

    /**
     * @return the byte offset of the event.
     */
    public String getOffset() {
        return this.offset;
    }

    /**
     * @return true if the current event will be included. false if the current event will not be included.
     */
    public Boolean getInclusive() {
        return this.inclusive;
    }

    /**
     * @return the sequence number of the event.
     */
    public Long getSequenceNumber() {
        return this.sequenceNumber;
    }

    /**
     * @return the enqueued time of the event.
     */
    public Instant getEnqueuedTime() {
        return this.dateTime;
    }

    String getFilterType() {
        if (this.offset != null) {
            return "offset";
        }

        if (this.sequenceNumber != null) {
            return "sequenceNumber";
        }

        if (this.dateTime != null) {
            return "enqueuedTime";
        }

        throw new IllegalArgumentException("No starting position was set.");
    }
}
