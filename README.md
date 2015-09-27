# Fluency
Yet another fluentd logger.

## Features

* Better performance
* Asynchronous / synchronous flush to Fluentd
* TCP / UDP heartbeat with Fluentd
* `Message` / `PackedForward` formats are available
* Failover with multiple Fluentds
* Ack response (not implemented yet)

## Install

### Gradle

    dependencies {
        compile 'org.komamitsu:fluency:0.0.1'
    }

### Maven

    <dependency>
        <groupId>org.komamitsu</groupId>
        <artifactId>fluency</artifactId>
        <version>0.0.1</version>
    </dependency>
 
 
## Usage
 
 	// Single Fluentd(localhost:24224)
 	//   - Asynchronous flush
 	//   - PackedForward format
    Fluency fluency = Fluency.defaultFluency();
    
    // Multiple Fluentd(localhost:24224, localhost:24225)
    //   - TCP heartbeat
    // Fluency fluency = Fluency.defaultFluency(
    //                     Arrays.asList(new InetSocketAddress(24224), new InetSocketAddress(24225)));
    
    Map<String, Object> event = new HashMap<String, Object>();
    event.put("name", "komamitsu");
    event.put("age", 42);
    event.put("rate", 3.14);
    fluency.emit("foo_db.bar_tbl", event);
    
    fluency.close();