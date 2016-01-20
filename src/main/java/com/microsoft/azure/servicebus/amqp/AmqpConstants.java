package com.microsoft.azure.servicebus.amqp;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.apache.qpid.proton.amqp.*;

public final class AmqpConstants
{
	private AmqpConstants() { }
	
	public static final String Apache = "apache.org";
    public static final String Vendor = "com.microsoft";
	
    public static final String AmqpAnnotationFormat = "amqp.annotation.%s >%s '%s'";
	public static final String OffsetAnnotationName = "x-opt-offset";
	public static final String ReceivedAtAnnotationName = "x-opt-enqueued-time";
	
	public static final Symbol PartitionKey = Symbol.getSymbol("x-opt-partition-key");
	public static final Symbol Offset = Symbol.getSymbol(AmqpConstants.OffsetAnnotationName);
	public static final Symbol SequenceNumber = Symbol.getSymbol("x-opt-sequence-number");
	public static final Symbol EnqueuedTimeUtc = Symbol.getSymbol("x-opt-enqueued-time");
	
	public static final Symbol StringFilter = Symbol.valueOf(AmqpConstants.Apache + ":selector-filter:string");
	public static final Symbol Epoch = Symbol.valueOf(AmqpConstants.Vendor + ":epoch");
	
	public static final long AmqpBatchMessageFormat = 2147563264L; 
}
