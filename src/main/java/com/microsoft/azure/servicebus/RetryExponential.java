package com.microsoft.azure.servicebus;

import java.time.*;
import java.util.*;

public final class RetryExponential extends RetryPolicy
{
	public RetryExponential(Duration minimumBackoff, Duration maximumBackoff, Duration deltaBackoff){
		
	}
	
	// Will be removed: demo compilation purpose only
	RetryExponential()
	{
	}
}
