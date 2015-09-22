package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.sender.retry.RetryInterval;
import org.komamitsu.fluency.sender.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class RetryableSender
        implements Sender
{
    private static final Logger LOG = LoggerFactory.getLogger(RetryableSender.class);
    private static final RetryInterval DEFAULT_RETRY_INTERVAL = new RetryInterval.Builder().build();
    private static final RetryStrategy DEFAULT_RETRY_STRATEGY = new ExponentialBackOffRetryStrategy();

    private final RetryInterval retryInterval;
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

    public RetryableSender(Sender baseSender, RetryInterval retryInterval, RetryStrategy retryStrategy)
    {
        // TODO: null check
        this.baseSender = baseSender;
        this.retryInterval = retryInterval;
        this.retryStrategy = retryStrategy;
    }

    public RetryableSender(Sender baseSender, RetryStrategy retryStrategy)
    {
        this(baseSender, DEFAULT_RETRY_INTERVAL, retryStrategy);
    }

    public RetryableSender(Sender baseSender, RetryInterval retryInterval)
    {
        this(baseSender, retryInterval, DEFAULT_RETRY_STRATEGY);
    }

    public RetryableSender(Sender baseSender)
    {
        this(baseSender, DEFAULT_RETRY_INTERVAL, DEFAULT_RETRY_STRATEGY);
    }

    @Override
    public void send(ByteBuffer data)
            throws RetryOverException
    {
        IOException firstException = null;

        int retry = 0;
        while (!retryInterval.isRetriedOver(retry)) {
            try {
                baseSender.send(data);
                return;
            }
            catch (IOException e) {
                firstException = e;
                LOG.warn("Sender failed to send data. sender=" + this + ", retry=" + retry, e);
            }

            try {
                TimeUnit.MILLISECONDS.sleep(retryStrategy.getNextIntervalMillis(retryInterval, retry));
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
