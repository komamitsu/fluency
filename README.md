# Fluency
[<img src="https://travis-ci.org/komamitsu/fluency.svg?branch=master"/>](https://travis-ci.org/komamitsu/fluency) [![Coverage Status](https://coveralls.io/repos/komamitsu/fluency/badge.svg?branch=master&service=github)](https://coveralls.io/github/komamitsu/fluency?branch=master)

Yet another fluentd logger.

## Features

* Better performance ([4 times faster than fluent-logger-java](https://gist.github.com/komamitsu/c1e4045fe2ddb108cfbf12d5f014b683))
* Asynchronous / synchronous flush to Fluentd
* TCP / UDP heartbeat with Fluentd
* `PackedForward` format
* Failover with multiple Fluentds
* Enable /disable ack response mode

## Install

### Gradle

```groovy
dependencies {
    compile 'org.komamitsu:fluency:1.3.0'
}
```

### Maven

```xml
<dependency>
    <groupId>org.komamitsu</groupId>
    <artifactId>fluency</artifactId>
    <version>1.3.0</version>
</dependency>
```
 
## Usage

### Create Fluency instance

#### For single Fluentd

```java
// Single Fluentd(localhost:24224 by default)
//   - TCP heartbeat (by default)
//   - Asynchronous flush (by default)
//   - Without ack response (by default)
//   - Flush interval is 600ms (by default)
//   - Initial chunk buffer size is 1MB (by default)
//   - Threshold chunk buffer size to flush is 4MB (by default)
//   - Max total buffer size is 16MB (by default)
//   - Use off heap memory for buffer pool (by default)
//   - Max retry of sending events is 8 (by default)
//   - Max wait until all buffers are flushed is 10 seconds (by default)
//   - Max wait until the flusher is terminated is 10 seconds (by default)
Fluency fluency = Fluency.defaultFluency();
```

#### For multiple Fluentd with failover

```java    
// Multiple Fluentd(localhost:24224, localhost:24225)
Fluency fluency = Fluency.defaultFluency(
			Arrays.asList(new InetSocketAddress(24224), new InetSocketAddress(24225)));
```

#### Enable ACK response mode

```java
// Single Fluentd(localhost:24224)
//   - With ack response
Fluency fluency = Fluency.defaultFluency(new Fluency.Config().setAckResponseMode(true));
```

#### Enable file backup mode

In this mode, Fluency takes backup of unsent memory buffers as files when closing and then resends them when restarting

```java
// Single Fluentd(localhost:24224)
//   - Backup directory is the temporary directory
Fluency fluency = Fluency.defaultFluency(new Fluency.Config().setFileBackupDir(System.getProperty("java.io.tmpdir")));
```

#### Buffer configuration

```java
// Single Fluentd(xxx.xxx.xxx.xxx:24224)
//   - Initial chunk buffer size = 4MB
//   - Threshold chunk buffer size to flush = 16MB
//     Keep this value (BufferRetentionSize) between `Initial chunk buffer size` and `Max total buffer size`
//   - Max total buffer size = 256MB
Fluency fluency = Fluency.defaultFluency("xxx.xxx.xxx.xxx", 24224,
	new Fluency.Config()
	    .setBufferChunkInitialSize(4 * 1024 * 1024)
	    .setBufferChunkRetentionSize(16 * 1024 * 1024)
	    .setMaxBufferSize(256 * 1024 * 1024L));
```

#### Waits on close sequence

```java
// Single Fluentd(localhost:24224)
//   - Max wait until all buffers are flushed is 30 seconds
//   - Max wait until the flusher is terminated is 40 seconds
Fluency fluency = Fluency.defaultFluency(
	new Fluency.Config()
        .setWaitUntilBufferFlushed(30)
        .setWaitUntilFlusherTerminated(40));
```

#### Register Jackson modules
```java
// Single Fluentd(localhost:24224)
//   - SimpleModule that has FooSerializer is enabled
Sender sender = new TCPSender.Config().createInstance();

SimpleModule simpleModule = new SimpleModule();
simpleModule.addSerializer(Foo.class, new FooSerializer());

PackedForwardBuffer.Config bufferConfig = new PackedForwardBuffer.Config()
	.setJacksonModules(Collections.<Module>singletonList(simpleModule));

Fluency fluency = new Fluency.Builder(sender).setBufferConfig(bufferConfig).build();
```

#### Set a custom error handler
```java
Fluency fluency = Fluency.defaultFluency(
        new Fluency.Config()
				.setSenderErrorHandler(new SenderErrorHandler()
				{
					@Override
					public void handle(Throwable e)
					{
						// Send a notification
					}
				}));
	:
// If flushing events to Fluentd fails and retried out, the error handler is called back.
fluency.emit("foo.bar", event);
```

#### Other configurations

```java
// Multiple Fluentd(localhost:24224, localhost:24225)
//   - Flush interval = 200ms
//   - Max retry of sending events = 12
//   - Use JVM heap memory for buffer pool
Fluency fluency = Fluency.defaultFluency(
			Arrays.asList(
				new InetSocketAddress(24224), new InetSocketAddress(24225)),
				new Fluency.Config().
					setFlushIntervalMillis(200).
					setSenderMaxRetryCount(12).
					setJvmHeapBufferMode(true));
```

### Emit event

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
int nanoSeconds;
    :
EventTime eventTime = EventTime.fromEpoch(epochSeconds, nanoSeconds);

// You can also create an EventTime object like this
// EventTime eventTime = EventTime.fromEpochMilli(System.currentTimeMillis());

fluency.emit(tag, eventTime, event);
```

### Release resources

```java
fluency.close();
```

### Know how much Fluency is allocating memory

```java
LOG.debug("Memory size allocated by Fluency is {}", fluency.getAllocatedBufferSize());
```

### Know how much Fluench is buffering unsent data in memory

```java
LOG.debug("Unsent data size buffered by Fluency in memory is {}", fluency.getBufferedDataSize());
```
