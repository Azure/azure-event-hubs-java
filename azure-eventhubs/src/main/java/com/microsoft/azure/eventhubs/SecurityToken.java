/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import java.time.Instant;
import java.util.Date;

public final class SecurityToken {

    private final String tokenType;
    private final String token;
    private final Date validTo;

    public SecurityToken(final String tokenType, final String token, final Date validTo) {
        this.tokenType = tokenType;
        this.token = token;
        this.validTo = validTo;
    }

    public String getTokenType() {
        return this.tokenType;
    }

    public String getToken() {
        return this.token;
    }

    public Date validTo() {
        return this.validTo;
    }
}
