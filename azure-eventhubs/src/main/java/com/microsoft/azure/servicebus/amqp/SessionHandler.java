/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus.amqp;

import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.function.Consumer;

import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.BaseHandler;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Session;

import com.microsoft.azure.servicebus.ClientConstants;

public class SessionHandler extends BaseHandler
{
	protected static final Logger TRACE_LOGGER = Logger.getLogger(ClientConstants.SERVICEBUS_CLIENT_TRACE);

	private final String entityName;
        private final Consumer<Session> onRemoteSessionOpen;
        private final Consumer<ErrorCondition> onRemoteSessionOpenError;
        
        private boolean sessionCreated = false;
        
	public SessionHandler(final String entityName, final Consumer<Session> onRemoteSessionOpen, final Consumer<ErrorCondition> onRemoteSessionOpenError)
	{
		this.entityName = entityName;
                this.onRemoteSessionOpenError = onRemoteSessionOpenError;
                this.onRemoteSessionOpen = onRemoteSessionOpen;
	}

	@Override
	public void onSessionRemoteOpen(Event e) 
	{
		if(TRACE_LOGGER.isLoggable(Level.FINE))
		{
			TRACE_LOGGER.log(Level.FINE, String.format(Locale.US, "entityName[%s], sessionIncCapacity[%s], sessionOutgoingWindow[%s]",
					this.entityName, e.getSession().getIncomingCapacity(), e.getSession().getOutgoingWindow()));
		}

		final Session session = e.getSession();
		if (session != null && session.getLocalState() == EndpointState.UNINITIALIZED)
		{
			session.open();
		}
                
                sessionCreated = true;
                if (this.onRemoteSessionOpen != null)
                        this.onRemoteSessionOpen.accept(session);
	}


	@Override 
	public void onSessionLocalClose(Event e)
	{
		if(TRACE_LOGGER.isLoggable(Level.FINE))
		{
			TRACE_LOGGER.log(Level.FINE, String.format(Locale.US, "entityName[%s], condition[%s]", this.entityName, 
					e.getSession().getCondition() == null ? "none" : e.getSession().getCondition().toString()));
		}
	}

	@Override
	public void onSessionRemoteClose(Event e)
	{ 
		if(TRACE_LOGGER.isLoggable(Level.FINE))
		{
			TRACE_LOGGER.log(Level.FINE, String.format(Locale.US, "entityName[%s], condition[%s]", this.entityName,
					e.getSession().getRemoteCondition() == null ? "none" : e.getSession().getRemoteCondition().toString()));
		}

		final Session session = e.getSession();
		if (session != null && session.getLocalState() != EndpointState.CLOSED)
		{
			session.close();
		}
                
                if (!sessionCreated && this.onRemoteSessionOpenError != null)
                        this.onRemoteSessionOpenError.accept(session.getRemoteCondition());
	}

	@Override
	public void onSessionFinal(Event e)
	{ 
		if(TRACE_LOGGER.isLoggable(Level.FINE))
		{
			TRACE_LOGGER.log(Level.FINE, String.format(Locale.US, "entityName[%s]", this.entityName));
		}
	}

}
