# Fluency
[<img src="https://travis-ci.org/komamitsu/fluency.svg?branch=master"/>](https://travis-ci.org/komamitsu/fluency) [![Coverage Status](https://coveralls.io/repos/komamitsu/fluency/badge.svg?branch=master&service=github)](https://coveralls.io/github/komamitsu/fluency?branch=master)

Yet another fluentd logger.

## Features

* Better performance ([3 times faster than fluent-logger-java](https://gist.github.com/komamitsu/781a8b519afdc553f50c))
* Asynchronous / synchronous flush to Fluentd
* TCP / UDP heartbeat with Fluentd
* `PackedForward` format
* Failover with multiple Fluentds
* Enable /disable ack response mode

## Install

### Gradle

```groovy
dependencies {
    compile 'org.komamitsu:fluency:0.0.12'
}
```

### Maven

```xml
<dependency>
    <groupId>org.komamitsu</groupId>
    <artifactId>fluency</artifactId>
    <version>0.0.12</version>
</dependency>
```
 
## Usage

### Create Fluency instance

#### For single Fluentd

```java
// Single Fluentd(localhost:24224)
//   - Asynchronous flush
//   - PackedForward format
//   - Without ack response
Fluency fluency = Fluency.defaultFluency();
```

#### For multiple Fluentd with failover

```java    
// Multiple Fluentd(localhost:24224, localhost:24225)
//   - TCP heartbeat
//   - Asynchronous flush
//   - PackedForward format
//   - Without ack response
Fluency fluency = Fluency.defaultFluency(
			Arrays.asList(new InetSocketAddress(24224), new InetSocketAddress(24225)));
```

#### Enable ACK response mode

```java
// Single Fluentd(localhost:24224)
//   - Asynchronous flush
//   - PackedForward format
//   - With ack response
Fluency fluency = Fluency.defaultFluency(new Fluency.Config().setAckResponseMode(true));
```

#### Enable file backup mode

In this mode, Fluency takes backup of unsent memory buffers as files when closing and then resends them when restarting

```java
// Single Fluentd(localhost:24224)
//   - Asynchronous flush
//   - PackedForward format
//   - Backup directory is the temporary directory
Fluency fluency = Fluency.defaultFluency(new Fluency.Config().setFileBackupDir(System.getProperty("java.io.tmpdir")));
```

#### Buffer configuration

```java
// Single Fluentd(xxx.xxx.xxx.xxx:24224)
//   - Asynchronous flush
//   - PackedForward format
//   - Initial chunk buffer size = 4MB (default: 1MB)
//   - Threshold chunk buffer size to flush = 16MB (default: 4MB)
//     Keep this value (BufferRetentionSize) between `Initial chunk buffer size` and `Max total buffer size`
//   - Max total buffer size = 256MB (default: 16MB)
Fluency fluency = Fluency.defaultFluency("xxx.xxx.xxx.xxx", 24224,
        new Fluency.Config()
            .setBufferChunkInitialSize(4 * 1024 * 1024)
            .setBufferChunkRetentionSize(16 * 1024 * 1024)
            .setMaxBufferSize(256 * 1024 * 1024L));
```


#### Other configurations

```java
// Multiple Fluentd(localhost:24224, localhost:24225)
//   - TCP heartbeat
//   - Asynchronous flush
//   - PackedForward format
//   - Without ack response
//   - Flush interval = 200ms (default: 600ms)
//   - Max retry of sending events = 12 (default: 8)
Fluency fluency = Fluency.defaultFluency(
			Arrays.asList(
	    			new InetSocketAddress(24224), new InetSocketAddress(24225)),
	    			new Fluency.Config().
	    				setFlushIntervalMillis(200).
	    				setSenderMaxRetryCount(12));
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

### Release resources

```java
fluency.close();
```

### Wait until all buffer is flushed

```java
fluency.close();
fluency.waitUntilFlushingAllBuffer(MAX_WAIT_BUF_FLUSH);
```

### Wait until Fluency is terminated. It's important for file backup mode

```java
fluency.close();
fluency.waitUntilFlusherTerminated(MAX_WAIT_FLUSHER_TERMINATED);
```

### Know how much Fluency is allocating memory

```java
LOG.debug("Memory size allocated by Fluency is {}", fluency.getAllocatedBufferSize());
```

### Know how much Fluench is buffering unsent data in memory

```java
LOG.debug("Unsent data size buffered by Fluency in memory is {}", fluency.getBufferedDataSize());
```
