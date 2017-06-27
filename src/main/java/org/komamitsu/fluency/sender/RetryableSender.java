package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.sender.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RetryableSender
        extends Sender
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

    protected RetryableSender(Config config)
    {
        super(config.getBaseConfig());
        baseSender = config.getBaseSenderConfig().createInstance();
        retryStrategy = config.getRetryStrategyConfig().createInstance();
    }

    @Override
    public boolean isAvailable()
    {
        return true;
    }

    @Override
    protected synchronized void sendInternal(List<ByteBuffer> dataList, byte[] ackToken)
            throws IOException
    {
        IOException firstException = null;

        int retry = 0;
        while (!retryStrategy.isRetriedOver(retry)) {
            if (isClosed.get()) {
                throw new RetryOverException("This sender is already closed", firstException);
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

    public Sender getBaseSender()
    {
        return baseSender;
    }

    public RetryStrategy getRetryStrategy()
    {
        return retryStrategy;
    }

    public boolean isClosed()
    {
        return isClosed.get();
    }

    @Override
    public String toString()
    {
        return "RetryableSender{" +
                "baseSender=" + baseSender +
                ", retryStrategy=" + retryStrategy +
                ", isClosed=" + isClosed +
                "} " + super.toString();
    }

    public static class Config
            implements Instantiator
    {
        private final Sender.Config baseConfig = new Sender.Config();
        private RetryStrategy.Instantiator retryStrategyConfig = new ExponentialBackOffRetryStrategy.Config();

        public Sender.Config getBaseConfig()
        {
            return baseConfig;
        }

        public SenderErrorHandler getSenderErrorHandler()
        {
            return baseConfig.getSenderErrorHandler();
        }

        public Config setSenderErrorHandler(SenderErrorHandler senderErrorHandler)
        {
            baseConfig.setSenderErrorHandler(senderErrorHandler);
            return this;
        }

        public Config(Instantiator baseSenderConfig)
        {
            this.baseSenderConfig = baseSenderConfig;
        }

        private final Instantiator baseSenderConfig;

        public Instantiator getBaseSenderConfig()
        {
            return baseSenderConfig;
        }

        public RetryStrategy.Instantiator getRetryStrategyConfig()
        {
            return retryStrategyConfig;
        }

        public Config setRetryStrategyConfig(RetryStrategy.Instantiator retryStrategyConfig)
        {
            this.retryStrategyConfig = retryStrategyConfig;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "baseConfig=" + baseConfig +
                    ", retryStrategyConfig=" + retryStrategyConfig +
                    ", baseSenderConfig=" + baseSenderConfig +
                    '}';
        }

        @Override
        public RetryableSender createInstance()
        {
            return new RetryableSender(this);
        }
    }
}
