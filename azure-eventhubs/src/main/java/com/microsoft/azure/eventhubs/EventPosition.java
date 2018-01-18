/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import com.microsoft.azure.eventhubs.amqp.AmqpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Defines a position of an {@link EventData} in the event hub partition.
 * The position can be an Offset, Sequence Number, or EnqueuedTime.
 */
public class EventPosition {

    private static final Logger TRACE_LOGGER = LoggerFactory.getLogger(EventPosition.class);

    private final String offset;
    private final Long sequenceNumber;
    private final Instant dateTime;
    private final Boolean inclusiveFlag;

    private EventPosition(String o, Long s, Instant d, Boolean i) {
        this.offset = o;
        this.sequenceNumber = s;
        this.dateTime = d;
        this.inclusiveFlag = i;
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
     * @param inclusiveFlag will include the specified event when set to true; otherwise, the next event is returned.
     * @return An {@link EventPosition} object.
     */
    public static EventPosition fromOffset(String offset, boolean inclusiveFlag) {
        return new EventPosition(offset, null, null, inclusiveFlag);
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
     * @param inclusiveFlag         will include the specified event when set to true; otherwise, the next event is returned.
     * @return An {@link EventPosition} object.
     */
    public static EventPosition fromSequenceNumber(Long sequenceNumber, boolean inclusiveFlag) {
        return new EventPosition(null, sequenceNumber, null, inclusiveFlag);
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
     * @param inclusiveFlag When true, the last event in the partition will be sent. When false,
     *                  the last event will be skipped, and only new events will be received.
     * @return An {@link EventPosition} with position set to the end of the Event Hubs stream.
     */
    public static EventPosition fromEndOfStream(boolean inclusiveFlag) {
        return new EventPosition(PartitionReceiver.END_OF_STREAM, null, null, inclusiveFlag);
    }

    /**
     * Gets the offset of the event at the position. It can be null if the position is created
     * from a sequence number or enqueued time.
     *
     * @return the byte offset of the event.
     */
    public String getOffset() {
        return this.offset;
    }

    /**
     * Indicates if the current event at the specified offset is included or not.
     * It is only applicable if offset or sequence number is set. If offset or
     * sequence number is not set, this will return null.
     *
     * @return the inclusive flag
     */
    public Boolean getInclusiveFlag() {
        return this.inclusiveFlag;
    }

    /**
     * Gets the sequence number of the event at the position. It can be null if the position is created
     * from an offset or enqueued time.
     *
     * @return the sequence number of the event.
     */
    public Long getSequenceNumber() {
        return this.sequenceNumber;
    }

    /**
     * Gets the enqueued time of the event at the position. It can be null if the position is created
     * from an offset or a sequence number.
     *
     * @return the enqueued time of the event.
     */
    public Instant getEnqueuedTime() {
        return this.dateTime;
    }

    String getExpression() {
        // order of preference
        if (this.offset != null) {
            return this.inclusiveFlag ?
                    String.format(AmqpConstants.AMQP_ANNOTATION_FORMAT, AmqpConstants.OFFSET_ANNOTATION_NAME, "=", this.offset) :
                    String.format(AmqpConstants.AMQP_ANNOTATION_FORMAT, AmqpConstants.OFFSET_ANNOTATION_NAME, StringUtil.EMPTY, this.offset);
        }

        if (this.sequenceNumber != null) {
            return this.inclusiveFlag ?
                    String.format(AmqpConstants.AMQP_ANNOTATION_FORMAT, AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME, "=", this.sequenceNumber) :
                    String.format(AmqpConstants.AMQP_ANNOTATION_FORMAT, AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME, StringUtil.EMPTY, this.sequenceNumber);
        }

        if (this.dateTime != null) {
            String ms;
            try {
                ms = Long.toString(this.dateTime.toEpochMilli());
            } catch (ArithmeticException ex) {
                ms = Long.toString(Long.MAX_VALUE);
                if (TRACE_LOGGER.isWarnEnabled()) {
                    TRACE_LOGGER.warn(
                            "receiver not yet created, action[createReceiveLink], warning[starting receiver from epoch+Long.Max]");
                }
            }
            return String.format(AmqpConstants.AMQP_ANNOTATION_FORMAT, AmqpConstants.ENQUEUED_TIME_UTC_ANNOTATION_NAME, StringUtil.EMPTY, ms);
        }

        throw new IllegalArgumentException("No starting position was set.");
    }

    @Override
    public String toString() {
        return String.format("offset[%s], sequenceNumber[%s], enqueuedTime[%s], inclusiveFlag[%s]",
                this.offset, this.sequenceNumber, this.dateTime.toEpochMilli(), this.inclusiveFlag);
    }
}
