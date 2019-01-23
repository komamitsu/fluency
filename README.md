# Fluency
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.komamitsu/fluency-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.komamitsu/fluency-core)
[<img src="https://travis-ci.org/komamitsu/fluency.svg?branch=master"/>](https://travis-ci.org/komamitsu/fluency) [![Coverage Status](https://coveralls.io/repos/komamitsu/fluency/badge.svg?branch=master&service=github)](https://coveralls.io/github/komamitsu/fluency?branch=master)

High throughput data ingestion logger to Fluentd and Treasure Data

This document is for version 2. If you're looking for a document for version 1, see [this](./README-v1.md).

## Ingestion to Fluentd

### Features

* Better performance ([4 times faster than fluent-logger-java](https://gist.github.com/komamitsu/c1e4045fe2ddb108cfbf12d5f014b683))
* Asynchronous flush
* Backup of buffered data on local disk
* TCP / UDP heartbeat with Fluentd
* `PackedForward` format
* Failover with multiple Fluentds
* Enable / disable ack response mode

### Install

#### Gradle

```groovy
dependencies {
    compile "org.komamitsu:fluency-core:${fluency.version}"
    compile "org.komamitsu:fluency-fluentd:${fluency.version}"
}
```

#### Maven

```xml
<dependency>
    <groupId>org.komamitsu</groupId>
    <artifactId>fluency-core</artifactId>
    <version>${fluency.version}</version>
</dependency>

<dependency>
    <groupId>org.komamitsu</groupId>
    <artifactId>fluency-fluentd</artifactId>
    <version>${fluency.version}</version>
</dependency>
```
 
### Usage

#### Create Fluency instance

##### For single Fluentd

```java
// Single Fluentd(localhost:24224 by default)
//   - TCP heartbeat (by default)
//   - Asynchronous flush (by default)
//   - Without ack response (by default)
//   - Flush attempt interval is 600ms (by default)
//   - Initial chunk buffer size is 1MB (by default)
//   - Threshold chunk buffer size to flush is 4MB (by default)
//   - Threshold chunk buffer retention time to flush is 1000 ms (by default)
//   - Max total buffer size is 512MB (by default)
//   - Use off heap memory for buffer pool (by default)
//   - Max retry of sending events is 8 (by default)
//   - Max wait until all buffers are flushed is 10 seconds (by default)
//   - Max wait until the flusher is terminated is 10 seconds (by default)
Fluency fluency = new FluencyBuilderForFluentd().build();
```

##### For multiple Fluentd with failover

```java    
// Multiple Fluentd(localhost:24224, localhost:24225)
Fluency fluency = new FluencyBuilderForFluentd().build(
        Arrays.asList(
                new InetSocketAddress(24224),
                new InetSocketAddress(24225)));
```

##### Enable ACK response mode

```java
// Single Fluentd(localhost:24224)
//   - With ack response
FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
builder.setAckResponseMode(true);
Fluency fluency = builder.build();
```

##### Enable file backup mode

In this mode, Fluency takes backup of unsent memory buffers as files when closing and then resends them when restarting

```java
// Single Fluentd(localhost:24224)
//   - Backup directory is the temporary directory
FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
builder.setFileBackupDir(System.getProperty("java.io.tmpdir"));
Fluency fluency = builder.build();
```

##### Buffer configuration

```java
// Single Fluentd(xxx.xxx.xxx.xxx:24224)
//   - Initial chunk buffer size = 4MB
//   - Threshold chunk buffer size to flush = 16MB
//     Keep this value (BufferRetentionSize) between `Initial chunk buffer size` and `Max total buffer size`
//   - Max total buffer size = 256MB
FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
builder.setBufferChunkInitialSize(4 * 1024 * 1024);
builder.setBufferChunkRetentionSize(16 * 1024 * 1024);
builder.setMaxBufferSize(256 * 1024 * 1024L);
Fluency fluency = builder.build("xxx.xxx.xxx.xxx", 24224);
```

##### Waits on close sequence

```java
// Single Fluentd(localhost:24224)
//   - Max wait until all buffers are flushed is 30 seconds
//   - Max wait until the flusher is terminated is 40 seconds
FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
builder.setWaitUntilBufferFlushed(30);
builder.setWaitUntilFlusherTerminated(40);
Fluency fluency = builder.build();
```

##### Register Jackson modules
```java
// Single Fluentd(localhost:24224)
//   - SimpleModule that has FooSerializer is enabled
SimpleModule simpleModule = new SimpleModule();
simpleModule.addSerializer(Foo.class, new FooSerializer());

FluentdRecordFormatter.Config recordFormatterConfig =
	new FluentdRecordFormatter.Config();

recordFormatterConfig.setJacksonModules(
	Collections.singletonList(simpleModule));

Fluency fluency = new FluencyBuilder().buildFromIngester(
        new FluentdRecordFormatter(recordFormatterConfig),
        new FluentdIngester(new TCPSender());
```

##### Set a custom error handler
```java
FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
builder.setErrorHandler(ex -> {
  // Send a notification
});
Fluency fluency = builder.build();

    :

// If flushing events to Fluentd fails and retried out, the error handler is called back.
fluency.emit("foo.bar", event);
```

##### Send requests over SSL/TLS

```java
// Single Fluentd(localhost:24224)
//   - Enable SSL/TLS
FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
builder.setSslEnabled(true);
Fluency fluency = builder.build();
```

If you want to use a custom truststore, specify the JKS file path using `-Djavax.net.ssl.trustStore` (and `-Djavax.net.ssl.trustStorePassword` if needed). You can create a custom truststore like this:

```
$ keytool -import -file server.crt -alias mytruststore -keystore truststore.jks
```

For server side configuration, see https://docs.fluentd.org/v1.0/articles/in_forward#how-to-enable-tls/ssl-encryption .


##### Other configurations

```java
// Multiple Fluentd(localhost:24224, localhost:24225)
//   - Flush attempt interval = 200ms
//   - Max retry of sending events = 12
//   - Use JVM heap memory for buffer pool
FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
builder.setFlushIntervalMillis(200);
builder.setSenderMaxRetryCount(12);
builder.setJvmHeapBufferMode(true);
Fluency fluency = builder.build(
        Arrays.asList(
                new InetSocketAddress(24224),
                new InetSocketAddress(24225));
```

#### Emit event

```java
String tag = "foo_db.bar_tbl";
Map<String, Object> event = new HashMap<String, Object>();
event.put("name", "komamitsu");
event.put("age", 42);
event.put("rate", 3.14);
fluency.emit(tag, event);
```

If you want to use [EventTime](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format) as a timestamp, call `Fluency#emit` with an `EventTime` object in the following way

```java
int epochSeconds;
int nanoseconds;
    :
EventTime eventTime = EventTime.fromEpoch(epochSeconds, nanoseconds);

// You can also create an EventTime object like this
// EventTime eventTime = EventTime.fromEpochMilli(System.currentTimeMillis());

fluency.emit(tag, eventTime, event);
```

#### Wait until buffered data is flushed and release resource 

```java
fluency.close();
```

#### Know how much Fluency is allocating memory

```java
LOG.debug("Memory size allocated by Fluency is {}", fluency.getAllocatedBufferSize());
```

#### Know how much Fluency is buffering unsent data in memory

```java
LOG.debug("Unsent data size buffered by Fluency in memory is {}", fluency.getBufferedDataSize());
```

## Ingestion to Treasure Data

### Features

* Asynchronous flush
* Backup of buffered data on local disk
* Automatic database/table creation

### Install

#### Gradle

```groovy
dependencies {
    compile "org.komamitsu:fluency-core:${fluency.version}"
    compile "org.komamitsu:fluency-treasuredata:${fluency.version}"
}
```

#### Maven

```xml
<dependency>
    <groupId>org.komamitsu</groupId>
    <artifactId>fluency-core</artifactId>
    <version>${fluency.version}</version>
</dependency>

<dependency>
    <groupId>org.komamitsu</groupId>
    <artifactId>fluency-treasuredata</artifactId>
    <version>${fluency.version}</version>
</dependency>
```
 
#### Create Fluency instance

##### Default configuration

```java
// Asynchronous flush (by default)
// Flush attempt interval is 600ms (by default)
// Initial chunk buffer size is 1MB (by default)
// Threshold chunk buffer size to flush is 4MB (by default)
// Threshold chunk buffer retention time to flush is 30000 ms (by default)
// Max total buffer size is 512MB (by default)
// Use off heap memory for buffer pool (by default)
// Max retry of sending events is 10 (by default)
// Max wait until all buffers are flushed is 10 seconds (by default)
// Max wait until the flusher is terminated is 10 seconds (by default)
Fluency fluency = new FluencyBuilderForTreasureData().build(yourApiKey);
```

##### For high throughput data ingestion with high latency

```java
// Initial chunk buffer size = 32MB
// Threshold chunk buffer size to flush = 256MB
// Threshold chunk buffer retention time to flush = 120 seconds
// Max total buffer size = 1024MB
FluencyBuilderForTreasureData builder = new FluencyBuilderForTreasureData();
builder.setBufferChunkInitialSize(32 * 1024 * 1024);
builder.setMaxBufferSize(1024 * 1024 * 1024L);
builder.setBufferChunkRetentionSize(256 * 1024 * 1024);
builder.setBufferChunkRetentionTimeMillis(120 * 1000);
Fluency fluency = builder.build(yourApiKey);
```

##### Customize Treasure Data endpoint

```java
Fluency fluency = new FluencyBuilderForTreasureData()
						.build(yourApiKey, tdEndpoint);
```

##### Other configurations

Some of other usages are same as ingestion to Fluentd. See `Ingestion to Fluentd > Usage` above.
