package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.sender.failuredetect.FailureDetectStrategy;
import org.komamitsu.fluency.sender.failuredetect.FailureDetector;
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
import java.util.Arrays;
import java.util.List;

public class MultiSender
        extends Sender
{
    private static final Logger LOG = LoggerFactory.getLogger(MultiSender.class);
    @VisibleForTesting
    final List<Tuple<TCPSender, FailureDetector>> sendersAndFailureDetectors = new ArrayList<Tuple<TCPSender, FailureDetector>>();

    public MultiSender(Config config)
    {
        super(config);
        for (TCPSender.Config senderConfig : config.getSenderConfigs()) {
            // TODO: This is something ugly....
            Heartbeater.Config hbConfig = config.getHeartbeaterConfig().dupDefaultConfig().setHost(senderConfig.getHost()).setPort(senderConfig.getPort());
            FailureDetector failureDetector = null;
            try {
                failureDetector = new FailureDetector(config.getFailureDetectStrategyConfig(), hbConfig);
            }
            catch (IOException e) {
                LOG.error(String.format("Failed to initialize. config=%s", config), e);
                throw new RuntimeException(e);
            }
            sendersAndFailureDetectors.add(new Tuple<TCPSender, FailureDetector>(senderConfig.createInstance(), failureDetector));
        }
    }

    @Override
    protected MultiSender.Config getConfig()
    {
        return (MultiSender.Config) config;
    }

    @Override
    protected synchronized void sendInternal(List<ByteBuffer> dataList, byte[] ackToken)
            throws AllNodesUnavailableException
    {
        for (Tuple<TCPSender, FailureDetector> senderAndFailureDetector : sendersAndFailureDetectors) {
            TCPSender sender = senderAndFailureDetector.getFirst();
            FailureDetector failureDetector = senderAndFailureDetector.getSecond();
            LOG.trace("send(): hb.host={}, hb.port={}, isAvailable={}", failureDetector.getHeartbeater().getHost(), failureDetector.getHeartbeater().getPort(), failureDetector.isAvailable());
            if (failureDetector.isAvailable()) {
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
                    failureDetector.onFailure(e);
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
        for (Tuple<TCPSender, FailureDetector> senderAndFailureDetector : sendersAndFailureDetectors) {
            TCPSender sender = senderAndFailureDetector.getFirst();
            FailureDetector failureDetector = senderAndFailureDetector.getSecond();
            try {
                sender.close();
            }
            catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                }
            }
            finally {
                try {
                    failureDetector.close();
                }
                catch (IOException e) {
                    if (firstException == null) {
                        firstException = e;
                    }
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

    public static class Config extends Sender.Config<MultiSender, MultiSender.Config>
    {
        private final List<TCPSender.Config> senderConfigs;
        private FailureDetectStrategy.Config failureDetectStrategyConfig = new PhiAccrualFailureDetectStrategy.Config();
        private Heartbeater.Config heartbeaterConfig = new TCPHeartbeater.Config();

        public Config(List<TCPSender.Config> senderConfigs)
        {
            this.senderConfigs = senderConfigs;
        }

        @Override
        protected Config self()
        {
            return this;
        }

        public List<TCPSender.Config> getSenderConfigs()
        {
            return senderConfigs;
        }

        public FailureDetectStrategy.Config getFailureDetectStrategyConfig()
        {
            return failureDetectStrategyConfig;
        }

        public Config setFailureDetectStrategyConfig(FailureDetectStrategy.Config failureDetectStrategyConfig)
        {
            this.failureDetectStrategyConfig = failureDetectStrategyConfig;
            return this;
        }

        public Heartbeater.Config getHeartbeaterConfig()
        {
            return heartbeaterConfig;
        }

        public Config setHeartbeaterConfig(Heartbeater.Config heartbeaterConfig)
        {
            this.heartbeaterConfig = heartbeaterConfig;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "senderConfigs=" + senderConfigs +
                    ", failureDetectStrategyConfig=" + failureDetectStrategyConfig +
                    ", heartbeaterConfig=" + heartbeaterConfig +
                    "} " + super.toString();
        }

        @Override
        public MultiSender createInstance()
        {
            return new MultiSender(this);
        }
    }
}

