package com.microsoft.azure.servicebus;

import java.time.Duration;
import java.util.concurrent.*;

/**
 * An abstraction for a Scheduler functionality - which can later be replaced by a light-weight Thread
 */
public final class Timer
{
	private static final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(0);
	
	private Timer() 
	{
	}

	public static void schedule(Runnable runnable, Duration runFrequency, TimerType timerType)
	{
		switch (timerType)
		{
			case OneTimeRun:
				executor.schedule(runnable, runFrequency.getSeconds(), TimeUnit.SECONDS);
				break;
			
			case RepeatRun:
				executor.scheduleWithFixedDelay(runnable, runFrequency.getSeconds(), runFrequency.getSeconds(), TimeUnit.SECONDS);
				break;
				
			default:
				throw new UnsupportedOperationException("TODO: Other timer patterns");
		}
	}
}
