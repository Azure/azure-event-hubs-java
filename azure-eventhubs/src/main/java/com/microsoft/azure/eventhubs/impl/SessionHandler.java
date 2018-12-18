/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.impl;

import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.TimeoutException;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.reactor.Reactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Locale;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class SessionHandler extends BaseHandler {
    protected static final Logger TRACE_LOGGER = LoggerFactory.getLogger(SessionHandler.class);

    private final String entityName;
    private final Consumer<Session> onRemoteSessionOpen;
    private final BiConsumer<ErrorCondition, Exception> onRemoteSessionOpenError;
    private final Duration openTimeout;

    private boolean sessionCreated = false;
    private boolean sessionOpenErrorDispatched = false;

    public SessionHandler(final String entityName,
                          final Consumer<Session> onRemoteSessionOpen,
                          final BiConsumer<ErrorCondition, Exception> onRemoteSessionOpenError,
                          final Duration openTimeout) {
        this.entityName = entityName;
        this.onRemoteSessionOpenError = onRemoteSessionOpenError;
        this.onRemoteSessionOpen = onRemoteSessionOpen;
        this.openTimeout = openTimeout;
    }

    @Override
    public void onSessionLocalOpen(Event e) {
        if (TRACE_LOGGER.isInfoEnabled()) {
            TRACE_LOGGER.info(String.format(Locale.US, "onSessionLocalOpen entityName[%s], condition[%s]", this.entityName,
                    e.getSession().getCondition() == null ? "none" : e.getSession().getCondition().toString()));
        }

        if (this.onRemoteSessionOpenError != null) {

            ReactorHandler reactorHandler = null;
            final Reactor reactor = e.getReactor();
            final Iterator<Handler> reactorEventHandlers = reactor.getHandler().children();
            while (reactorEventHandlers.hasNext()) {
                final Handler currentHandler = reactorEventHandlers.next();
                if (currentHandler instanceof ReactorHandler) {
                    reactorHandler = (ReactorHandler) currentHandler;
                    break;
                }
            }

            final ReactorDispatcher reactorDispatcher = reactorHandler.getReactorDispatcher();
            final Session session = e.getSession();

            try {
                reactorDispatcher.invoke((int) this.openTimeout.toMillis(), new SessionTimeoutHandler(session));
            } catch (IOException ignore) {
                if (TRACE_LOGGER.isWarnEnabled()) {
                    TRACE_LOGGER.warn(String.format(Locale.US, "onSessionLocalOpen entityName[%s], reactorDispatcherError[%s]",
                            this.entityName, ignore.getMessage()));
                }

                session.close();
                this.onRemoteSessionOpenError.accept(
                        null,
                        new EventHubException(
                                false,
                                String.format("onSessionLocalOpen entityName[%s], underlying IO of reactorDispatcher faulted with error: %s",
                                        this.entityName, ignore.getMessage()), ignore));
            }
        }
    }

    @Override
    public void onSessionRemoteOpen(Event e) {
        if (TRACE_LOGGER.isInfoEnabled()) {
            TRACE_LOGGER.info(String.format(Locale.US, "onSessionRemoteOpen entityName[%s], sessionIncCapacity[%s], sessionOutgoingWindow[%s]",
                    this.entityName, e.getSession().getIncomingCapacity(), e.getSession().getOutgoingWindow()));
        }

        final Session session = e.getSession();
        if (session != null && session.getLocalState() == EndpointState.UNINITIALIZED) {
            session.open();
        }

        sessionCreated = true;
        if (this.onRemoteSessionOpen != null)
            this.onRemoteSessionOpen.accept(session);
    }

    @Override
    public void onSessionLocalClose(Event e) {
        if (TRACE_LOGGER.isInfoEnabled()) {
            TRACE_LOGGER.info(String.format(Locale.US, "onSessionLocalClose entityName[%s], condition[%s]", this.entityName,
                    e.getSession().getCondition() == null ? "none" : e.getSession().getCondition().toString()));
        }
    }

    @Override
    public void onSessionRemoteClose(Event e) {
        if (TRACE_LOGGER.isInfoEnabled()) {
            TRACE_LOGGER.info(String.format(Locale.US, "onSessionRemoteClose entityName[%s], condition[%s]", this.entityName,
                    e.getSession().getRemoteCondition() == null ? "none" : e.getSession().getRemoteCondition().toString()));
        }

        final Session session = e.getSession();
        if (session != null && session.getLocalState() != EndpointState.CLOSED) {
            session.close();
        }

        this.sessionOpenErrorDispatched = true;
        if (!sessionCreated && this.onRemoteSessionOpenError != null)
            this.onRemoteSessionOpenError.accept(session.getRemoteCondition(), null);
    }

    @Override
    public void onSessionFinal(Event e) {
        if (TRACE_LOGGER.isInfoEnabled()) {
            TRACE_LOGGER.info(String.format(Locale.US, "onSessionFinal entityName[%s]", this.entityName));
        }
    }

    private class SessionTimeoutHandler extends DispatchHandler {

        private final Session session;

        public SessionTimeoutHandler(final Session session) {
            this.session = session;
        }

        @Override
        public void onEvent() {

            // notify - if connection or transport error'ed out before even session open completed
            if (!sessionCreated && !sessionOpenErrorDispatched) {
                if (TRACE_LOGGER.isWarnEnabled()) {
                    TRACE_LOGGER.warn(String.format(Locale.US, "SessionTimeoutHandler.onEvent closing a session" +
                            "due to a connection/transport error before session open was complete."));
                }

                final Connection connection = session.getConnection();

                if (connection != null) {

                    if (connection.getRemoteCondition() != null && connection.getRemoteCondition().getCondition() != null) {

                        session.close();
                        onRemoteSessionOpenError.accept(connection.getRemoteCondition(), null);
                        return;
                    }

                    final Transport transport = connection.getTransport();
                    if (transport != null && transport.getCondition() != null && transport.getCondition().getCondition() != null) {

                        session.close();
                        onRemoteSessionOpenError.accept(transport.getCondition(), null);
                        return;
                    }
                }

                session.close();
                onRemoteSessionOpenError.accept(null, new TimeoutException("session creation timed out."));
            }
        }
    }
}
