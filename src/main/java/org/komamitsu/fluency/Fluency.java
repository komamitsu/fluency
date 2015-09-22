package org.komamitsu.fluency;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.buffer.PackedForwardBuffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.sender.RetryableSender;
import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.sender.TCPSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Map;

public class Fluency
        implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Fluency.class);
    private final Buffer buffer;
    private final Flusher flusher;

    public static Fluency defaultFluency(String host, int port)
            throws IOException
    {
        return new Fluency.Builder(new RetryableSender(new TCPSender(host, port))).build();
    }

    private Fluency(Buffer buffer, Flusher flusher)
    {
        this.buffer = buffer;
        this.flusher = flusher;
    }

    public void emit(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        buffer.append(tag, timestamp, data);
        flusher.onUpdate();
    }

    public void emit(String tag, Map<String, Object> data)
            throws IOException
    {
        emit(tag, System.currentTimeMillis() / 1000, data);
    }

    @Override
    public void flush()
            throws IOException
    {
        flusher.flush();
    }

    @Override
    public void close()
            throws IOException
    {
        flusher.close();
    }

    public static class Builder
    {
        private final Sender sender;
        private Buffer buffer;
        private Flusher.Config flusherConfig;

        public Builder(Sender sender)
        {
            this.sender = sender;
        }

        public Builder setBuffer(Buffer buffer)
        {
            this.buffer = buffer;
            return this;
        }

        public Builder setFlusherConfig(Flusher.Config flusherConfig)
        {
            this.flusherConfig = flusherConfig;
            return this;
        }

        public Fluency build()
        {
            Buffer buffer = this.buffer != null ? this.buffer : new PackedForwardBuffer();
            Flusher.Config flusherConfig = this.flusherConfig != null ? this.flusherConfig : new AsyncFlusher.Config();
            Flusher flusher = flusherConfig.createInstance(buffer, sender);

            return new Fluency(buffer, flusher);
        }
    }
}
