package com.microsoft.azure.eventhubs;

import com.microsoft.azure.eventhubs.amqp.AmqpConstants;
import org.apache.qpid.proton.amqp.Symbol;

import java.util.HashMap;
import java.util.Map;

public class ClientInfo {
    private String productName = ClientConstants.PRODUCT_NAME;

    private String productVersion = ClientConstants.CURRENT_JAVACLIENT_VERSION;

    public static ClientInfo Default = new ClientInfo(null, null);

    public ClientInfo(final String productName, final String productVersion) {
        if (productName != null) {
            this.productName += "/" + productName;
        }

        if (productVersion != null) {
            this.productVersion += "/" + productVersion;
        }
    }

    public Map<Symbol, Object> getConnectionProperties()
    {
        final Map<Symbol, Object> connectionProperties = new HashMap<Symbol, Object>();
        connectionProperties.put(AmqpConstants.PRODUCT, productName);
        connectionProperties.put(AmqpConstants.VERSION, productVersion);
        connectionProperties.put(AmqpConstants.PLATFORM, ClientConstants.PLATFORM_INFO);
        connectionProperties.put(AmqpConstants.FRAMEWORK, ClientConstants.FRAMEWORK_INFO);
        return connectionProperties;
    }
}
