/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface ITokenProvider {

    CompletableFuture<SecurityToken> getToken(final String resource, final Duration timeout);
}
