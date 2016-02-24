/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package com.microsoft.azure.servicebus.amqp;

import java.util.*;
import java.util.logging.*;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.transport.*;
import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.reactor.*;

import com.microsoft.azure.servicebus.*;

/**
 * ServiceBus <-> ProtonReactor interaction handles all
 * amqp_connection/transport related events from reactor
 */
public final class ConnectionHandler extends BaseHandler
{

	private static final Logger TRACE_LOGGER = Logger.getLogger(ClientConstants.SERVICEBUS_CLIENT_TRACE);

	private final String hostname;
	private final String username;
	private final String password;
	private final IAmqpConnection messagingFactory;

	public ConnectionHandler(final IAmqpConnection messagingFactory, final String hostname, final String username, final String password)
	{
		add(new Handshaker());
		
		this.hostname = hostname;
		this.username = username;
		this.password = password;
		this.messagingFactory = messagingFactory;
	}

	@Override
	public void onConnectionInit(Event event)
	{
		Connection connection = event.getConnection();
		connection.setHostname(this.hostname + ":" + ClientConstants.AMQPS_PORT);
		connection.setContainer(UUID.randomUUID().toString());
		connection.open();
	}
	
	@Override
	public void onConnectionBound(Event event)
	{
		Transport transport = event.getTransport();

		SslDomain domain = makeDomain(SslDomain.Mode.CLIENT);
		Ssl ssl = transport.ssl(domain);

		Sasl sasl = transport.sasl();
		sasl.plain(this.username, this.password);
	}
	
	@Override
	public void onConnectionUnbound(Event event)
	{
		if (TRACE_LOGGER.isLoggable(Level.WARNING))
		{
			TRACE_LOGGER.log(Level.WARNING, "Connection.onConnectionUnbound: hostname[" + event.getConnection().getHostname() + "]");
		}
	}

	@Override
	public void onTransportError(Event event)
	{
		ErrorCondition condition = event.getTransport().getCondition();
		if (condition != null)
		{
			if (TRACE_LOGGER.isLoggable(Level.WARNING))
			{
				TRACE_LOGGER.log(Level.WARNING, "Connection.onTransportError: hostname[" + event.getConnection().getHostname() + "], error[" + condition.getDescription() + "]");
			}
		}
		else
		{
			if (TRACE_LOGGER.isLoggable(Level.WARNING))
			{
				TRACE_LOGGER.log(Level.WARNING, "Connection.onTransportError: hostname[" + event.getConnection().getHostname() + "], error[no description returned]");
			}
		}
		
		this.messagingFactory.onConnectionError(condition);
	}
	
	@Override
	public void onConnectionRemoteOpen(Event event)
	{
		if (TRACE_LOGGER.isLoggable(Level.FINE))
		{
			TRACE_LOGGER.log(Level.FINE, "Connection.onConnectionRemoteOpen: hostname[" + event.getConnection().getHostname() + "]");
		}
		
		this.messagingFactory.onOpenComplete(null);
	}

	private static SslDomain makeDomain(SslDomain.Mode mode)
	{
		SslDomain domain = Proton.sslDomain();
		domain.init(mode);

		// TODO: VERIFY_PEER_NAME support
		domain.setPeerAuthentication(SslDomain.VerifyMode.ANONYMOUS_PEER);
		return domain;
	}
}
