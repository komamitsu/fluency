## 0.0.9 (2016-04-04)

Features:

- Add file backup mode that takes backups of unsent memory buffer as file when Fluency is closing and then resends the backup files when restarting
- Add new API `Fluency#isTerminated` to check if the flusher is finished already

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
