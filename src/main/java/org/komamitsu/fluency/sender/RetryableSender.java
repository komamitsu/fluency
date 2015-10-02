package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.sender.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RetryableSender
        implements Sender
{
    private static final Logger LOG = LoggerFactory.getLogger(RetryableSender.class);

    private final RetryStrategy retryStrategy;
    private final Sender baseSender;

    @Override
    public void close()
            throws IOException
    {
        baseSender.close();
    }

    public static class RetryOverException
            extends IOException
    {
        public RetryOverException(String s, Throwable throwable)
        {
            super(s, throwable);
        }
    }

    public RetryableSender(Sender baseSender, RetryStrategy retryStrategy)
    {
        // TODO: null check
        this.baseSender = baseSender;
        this.retryStrategy = retryStrategy;
    }

    public RetryableSender(Sender baseSender)
    {
        this(baseSender, new ExponentialBackOffRetryStrategy.Config().createInstance());
    }

    @Override
    public void send(ByteBuffer data)
            throws IOException
    {
        sendInternal(Arrays.asList(data), null);
    }

    @Override
    public void send(List<ByteBuffer> dataList)
            throws IOException
    {
        sendInternal(dataList, null);
    }

    @Override
    public void sendWithAck(List<ByteBuffer> dataList, String uuid)
            throws IOException
    {
        sendInternal(dataList, uuid);
    }

    private synchronized void sendInternal(List<ByteBuffer> dataList, String uuid)
            throws IOException
    {
        IOException firstException = null;

        int retry = 0;
        while (!retryStrategy.isRetriedOver(retry)) {
            try {
                if (uuid == null) {
                    baseSender.send(dataList);
                }
                else {
                    baseSender.sendWithAck(dataList, uuid);
                }
                return;
            }
            catch (IOException e) {
                firstException = e;
                LOG.warn("Sender failed to send data. sender=" + this + ", retry=" + retry, e);
            }

            try {
                TimeUnit.MILLISECONDS.sleep(retryStrategy.getNextIntervalMillis(retry));
            }
            catch (InterruptedException e) {
                LOG.debug("Interrupted while waiting", e);
                Thread.currentThread().interrupt();
            }
            retry++;
        }

        throw new RetryOverException("Sending data was retried over", firstException);
    }

}
