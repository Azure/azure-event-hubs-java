/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

/**
 * All TransportType switches available for communicating to EventHubs service.
 */
public enum TransportType {
    /**
     * AMQP over TCP. Uses port 5671 - assigned by IANA for secure AMQP (AMQPS).
     */
    AMQP,

    /**
     * AMQP over Web Sockets. Uses port 443.
     */
    AMQP_WEB_SOCKETS
}