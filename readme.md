<p align="center">
  <img src="event-hubs.png" alt="Microsoft Azure Event Hubs" width="100"/>
</p>

# Microsoft Azure Event Hubs Client for Java

|Build/Package|Status|
|------|-------------|
|master|[![Build status](https://ci.appveyor.com/api/projects/status/3prh8sm3stn4o5vj/branch/master?svg=true)](https://ci.appveyor.com/project/jtaubensee/azure-event-hubs-java/branch/master)|
|dev|[![Build status](https://ci.appveyor.com/api/projects/status/3prh8sm3stn4o5vj/branch/dev?svg=true)](https://ci.appveyor.com/project/jtaubensee/azure-event-hubs-java/branch/dev)|
|azure-eventhubs|[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs)
|azure-eventhubs-eph|[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-eph/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.microsoft.azure/azure-eventhubs-eph)

Azure Event Hubs is a highly scalable publish-subscribe service that can ingest millions of events per second and stream them into multiple applications. This lets you process and analyze the massive amounts of data produced by your connected devices and applications. Once Event Hubs has collected the data, you can retrieve, transform and store it by using any real-time analytics provider or with batching/storage adapters. 

Refer to the [online documentation](https://azure.microsoft.com/services/event-hubs/) to learn more about Event Hubs in general.
Refer to [General Overview document](Overview.md) for a general overview of Event Hubs Client for Java.

## Samples
Additional samples are provided here: [azure/azure-event-hubs](https://github.com/Azure/azure-event-hubs/tree/master/samples)

## Using the library 

This library is available for use in Maven projects from the Maven Central Repository, and can be referenced using the
following dependency declaration. The dependency declaration will in turn pull further required dependencies, specifically 
the required version of Apache Qpid Proton-J, and the cryptography library BCPKIX by the Legion of Bouncy Castle.   

```XML
   	<dependency> 
   		<groupId>com.microsoft.azure</groupId> 
   		<artifactId>azure-eventhubs</artifactId> 
   		<version>0.15.1</version>
   	</dependency>
 ```
 
 For different types of build environments, the latest released JAR files can also be [explicitly obtained from the 
 Maven Central Repository]() or from [the Release distribution point on GitHub]().  

## How to provide feedback

First, if you experience any issues with the runtime behavior of the Azure Event Hubs service, please consider filing a support request
right away. Your options for [getting support are enumerated here](https://azure.microsoft.com/support/options/). In the Azure portal, 
you can file a support request from the "Help and support" menu in the upper right hand corner of the page.   

If you find issues in this library or have suggestions for improvement of code or documentation, you can [file an issue in the project's 
GitHub repository](https://github.com/Azure/azure-event-hubs/issues) or send across a pull request - see our [Contribution Guidelines](./.github/CONTRIBUTING.md). 

Issues related to runtime behavior of the service, such as sporadic exceptions or apparent service-side performance or reliability issues can not be handled here.

Generally, if you want to discuss Azure Event Hubs or this client library with the community and the maintainers, you can turn to 
[stackoverflow.com under the #azure-eventhub tag](http://stackoverflow.com/questions/tagged/azure-eventhub) or the 
[MSDN Service Bus Forum](https://social.msdn.microsoft.com/Forums/en-US/home?forum=servbus). 

## Build & contribute to the library

You will generally not have to build this client library yourself. This library is available on maven central.
If you have any specific requirement for which you want to contribute or need to generate a SNAPSHOT version, this section is for you.

We adopted maven build model and strive to keep the project model intuitive enough to developers. 
If you need any help with any specific IDE or cannot get the build going in any environment - please open an issue.
Here are few general topics, which we thought developers would need help with:

### Running Integration tests

Set these Environment variables to be able to run unit tests targeting Microsoft Azure EventHubs service:
  * EVENT_HUB_CONNECTION_STRING
  * EPHTESTSTORAGE

### Explore the client library with IDEs

* If you see any Build Errors - make sure the Execution Environment is set to JDK version 1.8 or higher

