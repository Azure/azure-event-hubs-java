/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus;

import java.util.*;
import java.util.concurrent.*;

import org.apache.qpid.proton.message.Message;
import com.microsoft.azure.eventhubs.*;

public abstract class ReceiveHandler
{
	
	public abstract void onReceiveMessages(LinkedList<Message> messages);

	public abstract void onError(Exception exception);
	
}
