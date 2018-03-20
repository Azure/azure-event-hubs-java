## How to use this repository

Step-1. In your pom.xml file add the below section.

```  
  <repositories>
    <repository>
      <id>POC SNAPSHOT repository for EventHubs client SDK to connect using websockets over proxies</id>
      <url>http://raw.github.com/Azure/azure-event-hubs-java/websocket.with.1_0/snapshots</url>
    </repository>
  </repositories>
```

Step-2. & add this section in `dependencies`

```
  <dependency>
    <groupId>com.microsoft.azure</groupId>
    <artifactId>azure-eventhubs</artifactId>
    <version>1.1.0-SNAPSHOT</version>
  </dependency>
```

Step-3. Configure proxy settings in your java application using the below code snippet. This code should precede `EventHubClient` creation.

```
  EventHubClientImpl.PROXY_HOST_NAME = "10.71.124.142"; // PROXY ADDRESS
  EventHubClientImpl.PROXY_HOST_PORT = 8888; // PROXY PORT
  EventHubClientImpl.PROXY_AUTHORIZATION_HEADER = "Basic MTox"; // PROXY AUTHORIZATION HEADER; for Basic auth it is - Basic Base64Encoding(username:password)
```

### General Notes

1. This repository is made for testing - support for `web sockets over proxy` & will be deleted once the web sockets support is ported to `master` branch & is not intended to run in Production workloads.
2. This SNAPSHOT jar is built with dependencies - and packages the dependency library proton-j - in which we implemented PROXY negotiation.