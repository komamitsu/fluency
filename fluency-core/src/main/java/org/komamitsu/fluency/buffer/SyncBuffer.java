package org.komamitsu.fluency.buffer;

import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class SyncBuffer
        implements Buffer, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SyncBuffer.class);

    private final RecordFormatter recordFormatter;

    private Ingester ingester;


    public SyncBuffer(final Ingester ingester, final RecordFormatter recordFormatter)
    {
        this.ingester = ingester;
        this.recordFormatter = recordFormatter;
    }

    public void flush(Ingester ingester, boolean force)
            throws IOException
    {
        // noop
    }

    @Override
    public void close()
    {
        try {
            ingester.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        appendMapInternal(tag, timestamp, data);
    }

    public void append(String tag, EventTime timestamp, Map<String, Object> data)
            throws IOException
    {
        appendMapInternal(tag, timestamp, data);
    }

    public void appendMessagePackMapValue(String tag, long timestamp, byte[] mapValue, int offset, int len)
            throws IOException
    {
        appendMessagePackMapValueInternal(tag, timestamp, mapValue, offset, len);
    }

    public void appendMessagePackMapValue(String tag, EventTime timestamp, byte[] mapValue, int offset, int len)
            throws IOException
    {
        appendMessagePackMapValueInternal(tag, timestamp, mapValue, offset, len);
    }

    public void appendMessagePackMapValue(String tag, long timestamp, ByteBuffer mapValue)
            throws IOException
    {
        appendMessagePackMapValueInternal(tag, timestamp, mapValue);
    }

    public void appendMessagePackMapValue(String tag, EventTime timestamp, ByteBuffer mapValue)
            throws IOException
    {
        appendMessagePackMapValueInternal(tag, timestamp, mapValue);
    }


    private void appendMapInternal(String tag, Object timestamp, Map<String, Object> data)
            throws IOException
    {
        ingester.ingest(tag, ByteBuffer.wrap(recordFormatter.format(tag, timestamp, data)));
    }

    private void appendMessagePackMapValueInternal(String tag, Object timestamp, byte[] mapValue, int offset, int len)
            throws IOException
    {
        ingester.ingest(tag, ByteBuffer.wrap(recordFormatter.formatFromMessagePack(tag, timestamp, mapValue, offset, len)));
    }

    private void appendMessagePackMapValueInternal(String tag, Object timestamp, ByteBuffer mapValue)
            throws IOException
    {
        ingester.ingest(tag, ByteBuffer.wrap(recordFormatter.formatFromMessagePack(tag, timestamp, mapValue)));
    }


    @Override
    public void clearBackupFiles()
    {
    }

    @Override
    public long getAllocatedSize() {
        return 0;
    }

    @Override
    public long getBufferedDataSize() {
        return 0;
    }
}