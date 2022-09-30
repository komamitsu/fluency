## 2.7.0 (2022-09-30)

Improvements:

- Avoid sending buffer chunks larger than `BufferChunkRetentionSize`

Misc:

- Build and publish Maven packages using Java 17 with `--release 8` option
- Add a new artifact `fluency-fluentd-ext` for Java 16 or later

Features:

- Support UNIX domain socket in `fluency-fluentd-ext`

## 2.6.5 (2022-07-03)

Bugfix:

- Improve error handling for disconnection in TCPSender and SSLSender

## 2.6.4 (2022-04-24)

Features:

- Add `FluencyBuilderForFluentd#setSslSocketFactory()`

## 2.6.3 (2022-02-03)

Misc:

- Shade jackson-dataformat-msgpack

## 2.6.2 (2022-01-25)

Bugfix:

- Rebuild with Java 8

## 2.6.1 (2022-01-15)

Features:

- Add `FluencyBuilderForFluentd#setRecordFormatter()`

## 2.6.0 (2021-06-27)

Features:

- Support mutual TLS authentication

Misc:

- Migrate Gradle build scripts to Kotlin DSL

## 2.5.1 (2021-02-13)

Bugfixes:

- The ack token is now encoded as string and not as byte array. See https://github.com/komamitsu/fluency/issues/181 

## 2.5.0 (2020-11-12)

Bugfixes:

- Fix a potential memory leak that could happen when Fluentd keeps down for long time

## 2.4.1 (2020-03-08)

Bugfixes:

- Fix a bug SSLSender sends wrong size data with JVM heap mode enabled

## 2.4.0 (2019-08-31)

Features:

- Rename `flushIntervalMillis` to `flushAttemptIntervalMillis` in fluency-fluentd
- Add `senderMaxRetryIntervalMillis` and `senderBaseRetryIntervalMillis` to `FluencyBuilderForFluentd

Security:

- Use jackson-dataformat-msgpack:2.9.9.3 through msgpack-java:0.8.18 to take care of some CVE

Misc:

- Replace JUnit4 tests with JUnit5

## 2.3.3 (2019-06-19)

Security:

- Use jackson-dataformat-msgpack:2.9.9 through msgpack-java:0.8.17 to take care of CVE-2017-7525 again

## 2.3.2 (2019-06-03)

Bugfixes:

- Fix a bug small buffered data that doesn't reach `chunkRetentionSize` is unexpectedly flushed
- Fix a bug that ignores `waitUntilTerminated` in `close()`

## 2.3.1 (2019-05-25)

Bugfixes:

- Remove logback.xml added unexpectedly

## 2.3.0 (2019-05-10)

Bugfixes:

- Fix potential socket leaks that could happen with connection failures except `java.net.ConnectException` and so on

## 2.2.1 (2019-04-23)

Features:

- Support data ingestion to AWS S3

Improvements:

- Set daemon property of executor threads to reduce potential risks that prevent the program from shuting down

## 2.1.0 (2019-02-16)

Refactoring:

- Divide the Gradle project into subprojects
- Refactor and improve fluency-treasuredata

## 2.0.0 (2019-01-23)

Features:

- Experimentally support data ingestion to Treasure Data

Refactoring:

- Extract FluencyBuilder from Fluency
- Merge PackedForwardBuffer into Buffer
- Add Ingester class layer

## 1.8.0 (2018-04-01)

Features:

- Support SSL/TLS
- Update dependencies

## 1.7.0 (2018-01-05)

Features:

- Add `size` field to `option` section of PackedForward requests

## 1.6.0 (2017-11-24)

Bugfixes:

- Improve closing sequence after Fluency#close is called
  - Call sender.close() at the end to prevent receiver from failing to read socket
  - Make TCPSender#close to wait unsent request is flushed

## 1.5.0 (2017-10-30)

Features:

- Add emit methods to output MessagePack encoded data directly

## 1.4.0 (2017-07-01)

Features:

- Support custom error handler that is called back when a send error occurs

## 1.3.0 (2017-05-03)

Bugfixes:

- Fix bug Fluency with SyncFlusher doesn't flush buffered events without manually calling flush()

## 1.2.0 (2017-04-09)

Features:

- Support EventTime https://github.com/komamitsu/fluency/pull/62
- Support buffer pool on JVM heap https://github.com/komamitsu/fluency/pull/63

## 1.1.0 (2017-01-14)

Optimizations:

- Change the following default values
  - `waitUntilBufferFlushed`: 10 -> 60 seconds
  - `waitUntilTerminated`: 10 -> 60 seconds

- Upgrade the version of `msgpack-java` from 0.8.9 to 0.8.11 which fixes some serious bugs

Bugfixes:

- Fix bug that a temporary thread invoked to release resources can prevent the process from exiting immediately

## 1.0.0 (2016-09-19)

Features:

- Add shadow jar to avoid jackson version incompatibility
- Support Jackson module registration to take care of https://github.com/komamitsu/fluency/issues/30
- Add Fluency.Config#setBufferChunkInitialSize and setBufferChunkRetentionSize
- Add Fluency#waitUntilFlusherTerminated
- Add Fluency.Config#setWaitUntilBufferFlushed and setWaitUntilFlusherTerminated
- Make Flusher appropriately wait until all buffers flushed

Refactoring:

- Rename Fluency#waitUntilFlushingAllBuffer to waitUntilAllBufferFlushed
- Remove ThreadLocal from PackedForwardBuffer
- Move FailureDetector to TCPSender from MultiSender
- Simplify Xxxxx.Config classes to address https://github.com/komamitsu/fluency/pull/33
- Simplify AsyncFlusher
- Improve the close sequence of Flusher, Buffer and Sender

## 0.0.12 (2016-08-08)

Optimizations:

- Reuse instances and reduce scope of Synchronization PackedForwardBuffer

Bugfixes:

- Fix bug PackedForwardBuffer#append occationally throws BufferOverflow exception

## 0.0.11 (2016-05-30)

Bugfixes:

- Fix bug PackedForwardBuffer#append occationally throws BufferOverflow exception

## 0.0.10 (2016-04-11)

Features:

- Add new API `Fluency#getAllocatedBufferSize` to know how much Fluency is allocating memory
- Add new API `Fluency#getBufferedDataSize` to know how much Fluency is buffering unsent data in memory
- Stop retry in RetryableSender after it is closed

## 0.0.9 (2016-04-04)

Features:

- Add `backup memory buffer to file` mode that does the followings
    - Takes backups of unsent memory buffer as file when Fluency is closing
    - Sends the backup files when starting
- Add new API `Fluency#isTerminated` to check if the flusher is finished already
- Remove `message` format

## 0.0.8 (2016-02-15)

Bugfixes:

- Fix bug that event transfer to Fluentd occasionally gets stuck with ACK response mode enabled when the Fluentd restarts

Refactoring:

- Make XXXXSender be created from its Config instance like other classes (e.g. Buffer class)

## 0.0.7 (2015-12-07)

Bugfixes:

- Not block when calling Fluency#emit and buffer is full

## 0.0.6 (2015-10-19)

Bugfixes:

- Fix the wrong calcuration of memory usage in BufferPool

## 0.0.5 (2015-10-19)

Features:

- Improve memory consumption with BufferPool
- Improve data transfer performance using writev(2)

## 0.0.4 (2015-10-12)

Features:

- Support `ack_response` mode
- Integrate with Travis CI
- Integrate with Coveralls
- Retry to send data when buffer is full

## 0.0.3 (2015-10-01)

Features:

- Analyze source code with findbugs

## 0.0.2 (2015-09-30)

Features:

- Support base features
    - PackForword and Message format
    - Sync / Async flush
    - Failover with multiple fluentds
    - TCP / UDP heartbeat
