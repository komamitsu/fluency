package org.komamitsu.fluency.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MultiSender
        extends Sender
{
    private static final Logger LOG = LoggerFactory.getLogger(MultiSender.class);
    private final List<Sender> senders = new ArrayList<Sender>();

    protected MultiSender(Config config)
    {
        super(config.getBaseConfig());
        for (Instantiator senderConfig : config.getSenderConfigs()) {
            senders.add(senderConfig.createInstance());
        }
    }

    @Override
    public boolean isAvailable()
    {
        return true;
    }

    @Override
    protected synchronized void sendInternal(List<ByteBuffer> dataList, byte[] ackToken)
            throws AllNodesUnavailableException
    {
        for (Sender sender : senders) {
            boolean isAvailable = sender.isAvailable();
            LOG.trace("send(): sender={}, isAvailable={}", sender, isAvailable);
            if (isAvailable) {
                try {
                    if (ackToken == null) {
                        sender.send(dataList);
                    }
                    else {
                        sender.sendWithAck(dataList, ackToken);
                    }
                    return;
                }
                catch (IOException e) {
                    LOG.error("Failed to send: sender=" + sender + ". Trying to use next sender...", e);
                }
            }
        }
        throw new AllNodesUnavailableException("All nodes are unavailable");
    }

    @Override
    public void close()
            throws IOException
    {
        IOException firstException = null;
        for (Sender sender : senders) {
            try {
                sender.close();
            }
            catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    public static class AllNodesUnavailableException
            extends IOException
    {
        public AllNodesUnavailableException(String s)
        {
            super(s);
        }
    }

    public List<Sender> getSenders()
    {
        return Collections.unmodifiableList(senders);
    }

    @Override
    public String toString()
    {
        return "MultiSender{" +
                "senders=" + senders +
                "} " + super.toString();
    }

    public static class Config
            implements Instantiator
    {
        private final Sender.Config baseConfig = new Sender.Config();
        private final List<Instantiator> senderConfigs;

        public Sender.Config getBaseConfig()
        {
            return baseConfig;
        }

        public ErrorHandler getErrorHandler()
        {
            return baseConfig.getErrorHandler();
        }

        public Config setErrorHandler(ErrorHandler errorHandler)
        {
            baseConfig.setErrorHandler(errorHandler);
            return this;
        }

        public Config(List<Instantiator> senderConfigs)
        {
            this.senderConfigs = senderConfigs;
        }

        public List<Instantiator> getSenderConfigs()
        {
            return senderConfigs;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "senderConfigs=" + senderConfigs +
                    "} " + super.toString();
        }

        @Override
        public MultiSender createInstance()
        {
            return new MultiSender(this);
        }
    }
}

