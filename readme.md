<p align="center">
  <img src="event-hubs.png" alt="Microsoft Azure Event Hubs" width="100"/>
</p>

# Microsoft Azure Event Hubs Client for Java

> Please note, a newer package [com.azure:azure-messaging-eventhubs](https://search.maven.org/artifact/com.azure/azure-messaging-eventhubs) for [Azure Event Hubs](https://azure.microsoft.com/services/event-hubs/) is available as of February 2020. While the packages `com.microsoft.azure:azure-eventhubs` and `com.microsoft.azure:azure-eventhubs-eph` will continue to receive critical bug fixes, we strongly encourage you to upgrade. Read the [migration guide](https://aka.ms/azsdk/java/migrate/eh) for more details.
> 
Azure Event Hubs is a highly scalable publish-subscribe service that can ingest millions of events per second and stream them into multiple applications. This lets you process and analyze the massive amounts of data produced by your connected devices and applications. Once Event Hubs has collected the data, you can retrieve, transform and store it by using any real-time analytics provider or with batching/storage adapters. 

The Azure Events Hubs client library for .NET allows for both sending and receiving of events.  Most common scenarios call for an application to act as either an event publisher or an event consumer, but rarely both. 

An **event publisher** is a source of telemetry data, diagnostics information, usage logs, or other log data, as 
part of an embedded device solution, a mobile device application, a game title running on a console or other device, 
some client or server based business solution, or a web site.  

An **event consumer** picks up such information from the Event Hub and processes it. Processing may involve aggregation, complex 
computation and filtering. Processing may also involve distribution or storage of the information in a raw or transformed fashion.
Event Hub consumers are often robust and high-scale platform infrastructure parts with built-in analytics capabilities, like Azure 
Stream Analytics, Apache Spark, or Apache Storm.  

## We've moved!

The Microsoft Azure Event Hubs Client for Java has joined the unified Azure Developer Platform and can now be found in the [Azure SDK for Java](https://github.com/Azure/azure-sdk-for-java/tree/master/sdk/eventhubs) repository.  To view the latest source, participate in the development process, report issues, or engage with the community, please visit our new home.

This repository has been archived and is intended to provide historical reference and context for the Microsoft Azure Event Hubs Client for Java.
  
[Source code](https://github.com/Azure/azure-sdk-for-java/tree/master/sdk/eventhubs) in the folders beginning with "microsoft-azure-" | [Client JAR (Maven)](https://mvnrepository.com/artifact/com.microsoft.azure/azure-eventhubs) | [Event Processor Host JAR (Maven)](https://mvnrepository.com/artifact/com.microsoft.azure/azure-eventhubs-eph) | [Product documentation](https://docs.microsoft.com/en-us/azure/event-hubs/)
