# Fluency
[<img src="https://travis-ci.org/komamitsu/fluency.svg?branch=master"/>](https://travis-ci.org/komamitsu/fluency) [![Coverage Status](https://coveralls.io/repos/komamitsu/fluency/badge.svg?branch=master&service=github)](https://coveralls.io/github/komamitsu/fluency?branch=master)

Yet another fluentd logger which also supoprts ingestion to Treasure Data.

## Ingestion to Fluentd

### Features

* Better performance ([4 times faster than fluent-logger-java](https://gist.github.com/komamitsu/c1e4045fe2ddb108cfbf12d5f014b683))
* Asynchronous flush
* Backup of buffered data on local disk
* TCP / UDP heartbeat with Fluentd
* `PackedForward` format
* Failover with multiple Fluentds
* Enable /disable ack response mode

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
import org.komamitsu.fluency.fluentd.FluencyBuilder;

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
Fluency fluency = FluencyBuilder.build();
```

##### For multiple Fluentd with failover

```java    
import org.komamitsu.fluency.fluentd.FluencyBuilder;

// Multiple Fluentd(localhost:24224, localhost:24225)
Fluency fluency = FluencyBuilder.build(
			Arrays.asList(new InetSocketAddress(24224), new InetSocketAddress(24225)));
```

##### Enable ACK response mode

```java
import org.komamitsu.fluency.fluentd.FluencyBuilder;

// Single Fluentd(localhost:24224)
//   - With ack response
Fluency fluency = FluencyBuilder.build(
                    new FluencyBuilder.FluencyConfig().setAckResponseMode(true));
```

##### Enable file backup mode

In this mode, Fluency takes backup of unsent memory buffers as files when closing and then resends them when restarting

```java
import org.komamitsu.fluency.fluentd.FluencyBuilder;

// Single Fluentd(localhost:24224)
//   - Backup directory is the temporary directory
Fluency fluency = FluencyBuilder.build(
                    new FluencyBuilder.FluencyConfig().setFileBackupDir(System.getProperty("java.io.tmpdir")));
```

##### Buffer configuration

```java
import org.komamitsu.fluency.fluentd.FluencyBuilder;

// Single Fluentd(xxx.xxx.xxx.xxx:24224)
//   - Initial chunk buffer size = 4MB
//   - Threshold chunk buffer size to flush = 16MB
//     Keep this value (BufferRetentionSize) between `Initial chunk buffer size` and `Max total buffer size`
//   - Max total buffer size = 256MB
Fluency fluency = FluencyBuilder.build("xxx.xxx.xxx.xxx", 24224,
                    new FluencyBuilder.FluencyConfig()
                        .setBufferChunkInitialSize(4 * 1024 * 1024)
                        .setBufferChunkRetentionSize(16 * 1024 * 1024)
                        .setMaxBufferSize(256 * 1024 * 1024L));
```

##### Waits on close sequence

```java
import org.komamitsu.fluency.fluentd.FluencyBuilder;

// Single Fluentd(localhost:24224)
//   - Max wait until all buffers are flushed is 30 seconds
//   - Max wait until the flusher is terminated is 40 seconds
Fluency fluency = FluencyBuilder.build(
	new FluencyBuilder.FluencyConfig()
        .setWaitUntilBufferFlushed(30)
        .setWaitUntilFlusherTerminated(40));
```

##### Register Jackson modules
```java
import org.komamitsu.fluency.fluentd.FluencyBuilder;

// Single Fluentd(localhost:24224)
//   - SimpleModule that has FooSerializer is enabled
FluentdSender sender = new TCPSender.Config().createInstance();
Ingester ingester = new FluentdIngester.Config().createInstance(sender);

SimpleModule simpleModule = new SimpleModule();
simpleModule.addSerializer(Foo.class, new FooSerializer());

Buffer.Config bufferConfig = new Buffer.Config();

FluentdRecordFormatter.Config fluentdRecordFormatterWithModuleConfig =
            new FluentdRecordFormatter.Config().setJacksonModules(Collections.singletonList(simpleModule));

Fluency fluency = BaseFluencyBuilder.buildFromConfigs(
        fluentdRecordFormatterWithModuleConfig,
        bufferConfig,
        new AsyncFlusher.Config(),
        ingester);
```

##### Set a custom error handler
```java
import org.komamitsu.fluency.fluentd.FluencyBuilder;

Fluency fluency = FluencyBuilder.build(
                    new FluencyBuilder.FluencyConfig()
                        .setErrorHandler(ex -> {
                            // Send a notification
                        }));
	:

// If flushing events to Fluentd fails and retried out, the error handler is called back.
fluency.emit("foo.bar", event);
```

##### Send requests over SSL/TLS

```java
import org.komamitsu.fluency.fluentd.FluencyBuilder;

// Single Fluentd(localhost:24224)
//   - Enable SSL/TLS
Fluency fluency = FluencyBuilder.build(
	new FluencyBuilder.FluencyConfig().setUseSsl(true));
```

If you want to use a custom truststore, specify the JKS file path using `-Djavax.net.ssl.trustStore` (and `-Djavax.net.ssl.trustStorePassword` if needed). You can create a custom truststore like this:

```
$ keytool -import -file server.crt -alias mytruststore -keystore truststore.jks
```

For server side configuration, see https://docs.fluentd.org/v1.0/articles/in_forward#how-to-enable-tls/ssl-encryption .


##### Other configurations

```java
import org.komamitsu.fluency.fluentd.FluencyBuilder;

// Multiple Fluentd(localhost:24224, localhost:24225)
//   - Flush attempt interval = 200ms
//   - Max retry of sending events = 12
//   - Use JVM heap memory for buffer pool
Fluency fluency = FluencyBuilder.build(
			Arrays.asList(
				new InetSocketAddress(24224), new InetSocketAddress(24225)),
				new FluencyBuilder.FluencyConfig().
					setFlushIntervalMillis(200).
					setSenderMaxRetryCount(12).
					setJvmHeapBufferMode(true));
```

##### Emit event

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

#### Release resources

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
import org.komamitsu.fluency.treasuredata.FluencyBuilder;

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
Fluency fluency = FluencyBuilder.build(yourApiKey);
```

##### For high throughput data ingestion with high latency

```java
import org.komamitsu.fluency.treasuredata.FluencyBuilder;

// Initial chunk buffer size = 32MB
// Threshold chunk buffer size to flush = 256MB
// Threshold chunk buffer retention time to flush = 120 seconds
// Max total buffer size = 1024MB
Fluency fluency = FluencyBuilder.build(yourApiKey
                        new FluencyBuilder.FluencyConfig()
                            .setBufferChunkInitialSize(32 * 1024 * 1024)
                            .setMaxBufferSize(1024 * 1024 * 1024L)
                            .setBufferChunkRetentionSize(256 * 1024 * 1024)
                            .setBufferChunkRetentionTimeMillis(120 * 1000));
```

##### Customize Treasure Data endpoint

```java
import org.komamitsu.fluency.treasuredata.FluencyBuilder;

Fluency fluency = FluencyBuilder.build(yourApiKey, tdEndpoint);
```

