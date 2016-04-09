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
import java.util.concurrent.atomic.AtomicBoolean;

public class RetryableSender
        extends Sender<RetryableSender.Config>
{
    private static final Logger LOG = LoggerFactory.getLogger(RetryableSender.class);
    private final Sender baseSender;
    private RetryStrategy retryStrategy;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    @Override
    public void close()
            throws IOException
    {
        baseSender.close();
        isClosed.set(true);
    }

    public static class RetryOverException
            extends IOException
    {
        public RetryOverException(String s, Throwable throwable)
        {
            super(s, throwable);
        }
    }

    public RetryableSender(Config config)
    {
        super(config);
        baseSender = config.getBaseSenderConfig().createInstance();
        retryStrategy = config.getRetryStrategyConfig().createInstance();
    }

    @Override
    protected synchronized void sendInternal(List<ByteBuffer> dataList, byte[] ackToken)
            throws IOException
    {
        IOException firstException = null;

        int retry = 0;
        while (!retryStrategy.isRetriedOver(retry)) {
            if (isClosed.get()) {
                LOG.warn("This sender is already closed");
                return;
            }

            try {
                if (ackToken == null) {
                    baseSender.send(dataList);
                }
                else {
                    baseSender.sendWithAck(dataList, ackToken);
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

    @Override
    public String toString()
    {
        return "RetryableSender{" +
                "baseSender=" + baseSender +
                ", retryStrategy=" + retryStrategy +
                "} " + super.toString();
    }

    public static class Config extends Sender.Config<RetryableSender, Config>
    {
        private RetryStrategy.Config retryStrategyConfig = new ExponentialBackOffRetryStrategy.Config();

        public Config(Sender.Config baseSenderConfig)
        {
            this.baseSenderConfig = baseSenderConfig;
        }

        private final Sender.Config baseSenderConfig;

        public Sender.Config getBaseSenderConfig()
        {
            return baseSenderConfig;
        }

        public RetryStrategy.Config getRetryStrategyConfig()
        {
            return retryStrategyConfig;
        }

        public Config setRetryStrategyConfig(RetryStrategy.Config retryStrategyConfig)
        {
            this.retryStrategyConfig = retryStrategyConfig;
            return this;
        }

        @Override
        public RetryableSender createInstance()
        {
            return new RetryableSender(this);
        }
    }
}
