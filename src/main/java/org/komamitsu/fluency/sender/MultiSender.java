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
import java.util.List;

public class MultiSender
        implements Sender
{
    private static final Logger LOG = LoggerFactory.getLogger(MultiSender.class);
    @VisibleForTesting
    final List<Tuple<TCPSender, FailureDetector>> sendersAndFailureDetectors = new ArrayList<Tuple<TCPSender, FailureDetector>>();

    public MultiSender(List<TCPSender> senders, FailureDetectStrategy.Config failureDetectStrategyConfig, Heartbeater.Config heartbeaterConfig)
            throws IOException
    {
        for (TCPSender sender : senders) {
            // TODO: This is something ugly....
            Heartbeater.Config config = heartbeaterConfig.dupDefaultConfig();
            config.setHost(sender.getHost());
            config.setPort(sender.getPort());
            config.setIntervalMillis(heartbeaterConfig.getIntervalMillis());

            FailureDetector failureDetector = new FailureDetector(failureDetectStrategyConfig, config);
            sendersAndFailureDetectors.add(new Tuple<TCPSender, FailureDetector>(sender, failureDetector));
        }
    }

    public MultiSender(List<TCPSender> senders, Heartbeater.Config heartbeaterConfig)
            throws IOException
    {
        this(senders, new PhiAccrualFailureDetectStrategy.Config(), heartbeaterConfig);
    }

    public MultiSender(List<TCPSender> senders)
            throws IOException
    {
        this(senders, new PhiAccrualFailureDetectStrategy.Config(), new TCPHeartbeater.Config());
    }

    @Override
    public void send(ByteBuffer data)
            throws IOException
    {
        for (Tuple<TCPSender, FailureDetector> senderAndFailureDetector : sendersAndFailureDetectors) {
            TCPSender sender = senderAndFailureDetector.getFirst();
            FailureDetector failureDetector = senderAndFailureDetector.getSecond();
            LOG.trace("send(): hb.host={}, hb.port={}, isAvailable={}", failureDetector.getHeartbeater().getHost(), failureDetector.getHeartbeater().getPort(), failureDetector.isAvailable());
            if (failureDetector.isAvailable()) {
                try {
                    sender.send(data);
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
}

