## 1.1.0-SNAPSHOT (unreleased)

Requires fluentd >= 0.14

Features:

- Add support for nanosecond timestamps

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
