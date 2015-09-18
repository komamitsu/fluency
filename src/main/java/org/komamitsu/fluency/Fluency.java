package org.komamitsu.fluency;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.buffer.BufferConfig;
import org.komamitsu.fluency.buffer.PackedForwardBuffer;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.flusher.FlusherConfig;
import org.komamitsu.fluency.flusher.SyncFlusher;
import org.komamitsu.fluency.sender.RetryableSender;
import org.komamitsu.fluency.sender.Sender;
import org.komamitsu.fluency.sender.TCPSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class Fluency
        implements Flushable, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Fluency.class);
    private final Sender sender;
    private final Buffer buffer;
    private final Flusher flusher;

    public static Fluency defaultFluency(String host, int port)
            throws IOException
    {
        return new Fluency.Builder(new RetryableSender(new TCPSender(host, port))).build();
    }

    private Fluency(Sender sender, Buffer buffer, Flusher flusher)
    {
        this.sender = sender;
        this.buffer = buffer;
        this.flusher = flusher;
    }

    public void emit(String tag, long timestamp, Map<String, Object> data)
            throws IOException
    {
        buffer.append(tag, timestamp, data);
        flusher.onUpdate(this, buffer);
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
        buffer.flush(sender);
    }

    @Override
    public void close()
            throws IOException
    {
        flusher.flush(this, buffer);
        sender.close();
    }

    public static class Builder
    {
        private final Sender sender;
        private Class<? extends Buffer> bufferClass = PackedForwardBuffer.class;
        private BufferConfig bufferConfig = new BufferConfig.Builder().build();
        private Class<? extends Flusher> flusherClass = SyncFlusher.class;
        private FlusherConfig flusherConfig = new FlusherConfig.Builder().build();
        public Builder(Sender sender)
        {
            this.sender = sender;
        }

        public Builder setBufferClass(Class<? extends Buffer> bufferClass)
        {
            this.bufferClass = bufferClass;
            return this;
        }

        public Builder setFlusherClass(Class<? extends Flusher> flusherClass)
        {
            this.flusherClass = flusherClass;
            return this;
        }

        public Builder setBufferConfig(BufferConfig bufferConfig)
        {
            this.bufferConfig = bufferConfig;
            return this;
        }

        public Builder setFlusherConfig(FlusherConfig flusherConfig)
        {
            this.flusherConfig = flusherConfig;
            return this;
        }

        public Fluency build()
        {
            Exception exception = null;
            try {
                Constructor<? extends Buffer> bufferConstructor = bufferClass.getConstructor(BufferConfig.class);
                Buffer buffer = bufferConstructor.newInstance(bufferConfig);

                Constructor<? extends Flusher> flusherConstructor = flusherClass.getConstructor(FlusherConfig.class);
                Flusher flusher = flusherConstructor.newInstance(flusherConfig);

                return new Fluency(sender, buffer, flusher);
            }
            catch (NoSuchMethodException e) {
                exception = e;
            }
            catch (InvocationTargetException e) {
                exception = e;
            }
            catch (InstantiationException e) {
                exception = e;
            }
            catch (IllegalAccessException e) {
                exception = e;
            }
            throw new IllegalStateException("Failed to build an instance. bufferClass=" + bufferClass + ", flusherClass=" + flusherClass, exception);
        }
    }
}
