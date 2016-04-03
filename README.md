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

    dependencies {
        compile 'org.komamitsu:fluency:0.0.9'
    }

### Maven

    <dependency>
        <groupId>org.komamitsu</groupId>
        <artifactId>fluency</artifactId>
        <version>0.0.9</version>
    </dependency>
 
 
## Usage

### Create Fluency instance

#### For single Fluend
 
 	// Single Fluentd(localhost:24224)
 	//   - Asynchronous flush
 	//   - PackedForward format
 	//   - Without ack response
    Fluency fluency = Fluency.defaultFluency();

#### For multiple Fluentd with failover
    
    // Multiple Fluentd(localhost:24224, localhost:24225)
    //   - TCP heartbeat
 	//   - Asynchronous flush
 	//   - PackedForward format
 	//   - Without ack response
    Fluency fluency = Fluency.defaultFluency(Arrays.asList(
    					new InetSocketAddress(24224), new InetSocketAddress(24225)));

#### Enable ACK response mode

 	// Single Fluentd(localhost:24224)
 	//   - Asynchronous flush
 	//   - PackedForward format
 	//   - With ack response
    Fluency fluency = Fluency.defaultFluency(new Fluency.Config().setAckResponseMode(true));

#### Enable file backup mode

In this mode, Fluency takes backup of unsent memory buffers as files when closing and then resends them when restarting

 	// Single Fluentd(localhost:24224)
 	//   - Asynchronous flush
 	//   - PackedForward format
 	//   - Backup directory is the temporary directory
    Fluency fluency = Fluency.defaultFluency(new Fluency.Config().setFileBackupDir(System.getProperty("java.io.tmpdir")));

#### Other configurations

    // Multiple Fluentd(localhost:24224, localhost:24225)
    //   - TCP heartbeat
 	//   - Asynchronous flush
 	//   - PackedForward format
 	//   - Without ack response
 	//   - Max buffer size = 32MB (default: 16MB)
 	//   - Flush interval = 200ms (default: 600ms)
 	//   - Max retry of sending events = 12 (default: 8)
    Fluency fluency = Fluency.defaultFluency(Arrays.asList(
    					new InetSocketAddress(24224), new InetSocketAddress(24225)),
    					new Fluency.Config().
    						setMaxBufferSize(32 * 1024 * 1024).
    						setFlushIntervalMillis(200).
    						setSenderMaxRetryCount(12));

#### Advanced configuration

	Sender sender = new MultiSender(
			                Arrays.asList(new TCPSender(24224), new TCPSender(24225)), 
			                new PhiAccrualFailureDetectStrategy.Config().setPhiThreshold(80),
		    	            new UDPHeartbeater.Config().setIntervalMillis(500));
	Buffer.Config bufferConfig = new MessageBuffer.Config();
	Flusher.Config flusherConfig = new SyncFlusher.Config().setBufferOccupancyThreshold(0.5f);
	Fluency fluency = new Fluency.Builder(sender).
							setBufferConfig(bufferConfig).
     						setFlusherConfig(flusherConfig).build();
        
### Emit event

    String tag = "foo_db.bar_tbl";
    Map<String, Object> event = new HashMap<String, Object>();
    event.put("name", "komamitsu");
    event.put("age", 42);
    event.put("rate", 3.14);
    fluency.emit(tag, event);

### Release resources

    fluency.close();

### Check if Fluency is terminated

    fluency.close();
    for (int i = 0; i < MAX_CHECK_TERMINATE; i++) {
        if (fluency.isTerminated()) {
        	break;
        }
        TimeUnit.SECONDS.sleep(CHECK_TERMINATE_INTERVAL);
    }

