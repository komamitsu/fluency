package org.komamitsu.fluency.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class Sender
    implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(Sender.class);
    private final Config config;

    protected Sender(Config config)
    {
        this.config = config;
    }

    public synchronized void send(ByteBuffer data)
            throws IOException
    {
        sendInternalWithRestoreBufferPositions(Arrays.asList(data), null);
    }

    public synchronized void send(List<ByteBuffer> dataList)
            throws IOException
    {
        sendInternalWithRestoreBufferPositions(dataList, null);
    }

    public void sendWithAck(List<ByteBuffer> dataList, byte[] ackToken)
            throws IOException
    {
        sendInternalWithRestoreBufferPositions(dataList, ackToken);
    }

    private void sendInternalWithRestoreBufferPositions(List<ByteBuffer> dataList, byte[] ackToken)
            throws IOException
    {
        List<Integer> positions = new ArrayList<Integer>(dataList.size());
        for (ByteBuffer data : dataList) {
            positions.add(data.position());
        }

        try {
            sendInternal(dataList, ackToken);
        }
        catch (Exception e) {
            for (int i = 0; i < dataList.size(); i++) {
                dataList.get(i).position(positions.get(i));
            }

            if (config.errorHandler != null) {
                try {
                    config.errorHandler.handle(e);
                }
                catch (Exception ex) {
                    LOG.warn("Failed to handle an error in the error handler {}", config.errorHandler, ex);
                }
            }

            if (e instanceof IOException) {
                throw (IOException)e;
            }
            else {
                throw new IOException(e);
            }
        }
    }

    public abstract boolean isAvailable();

    abstract protected void sendInternal(List<ByteBuffer> dataList, byte[] ackToken) throws IOException;

    public static class Config
    {
        private ErrorHandler errorHandler;

        public ErrorHandler getErrorHandler()
        {
            return errorHandler;
        }

        public Config setErrorHandler(ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "errorHandler=" + errorHandler +
                    '}';
        }
    }

    public interface Instantiator
    {
        Sender createInstance();
    }
}
