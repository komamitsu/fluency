package org.komamitsu.fluency.buffer;

import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.ingester.Ingester;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public interface Buffer extends Closeable {
    void flush(Ingester ingester, boolean force)
            throws IOException;

    void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException;

    void append(String tag, EventTime timestamp, Map<String, Object> data)
            throws IOException;

    void appendMessagePackMapValue(String tag, long timestamp, byte[] mapValue, int offset, int len)
            throws IOException;

    void appendMessagePackMapValue(String tag, EventTime timestamp, byte[] mapValue, int offset, int len)
            throws IOException;

    void appendMessagePackMapValue(String tag, long timestamp, ByteBuffer mapValue)
            throws IOException;

    void appendMessagePackMapValue(String tag, EventTime timestamp, ByteBuffer mapValue)
            throws IOException;

    void clearBackupFiles();

    long getAllocatedSize();

    long getBufferedDataSize();
}
