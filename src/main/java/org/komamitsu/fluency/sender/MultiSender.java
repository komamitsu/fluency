package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.sender.failuredetect.FailureDetectStrategy;
import org.komamitsu.fluency.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.sender.heartbeat.Heartbeater;
import org.komamitsu.fluency.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.util.Tuple;
import org.msgpack.core.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MultiSender
        extends Sender<MultiSender.Config>
{
    private static final Logger LOG = LoggerFactory.getLogger(MultiSender.class);
    @VisibleForTesting
    final List<Sender> senders = new ArrayList<Sender>();

    public MultiSender(Config config)
    {
        super(config);
        for (Sender.Config senderConfig : config.getSenderConfigs()) {
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

    public static class Config extends Sender.Config<MultiSender, Config>
    {
        private final List<Sender.Config> senderConfigs;

        public Config(List<Sender.Config> senderConfigs)
        {
            this.senderConfigs = senderConfigs;
        }

        public List<Sender.Config> getSenderConfigs()
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

