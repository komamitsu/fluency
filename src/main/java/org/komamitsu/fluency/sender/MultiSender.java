package org.komamitsu.fluency.sender;

import org.komamitsu.fluency.sender.failuredetect.FailureDetectStrategy;
import org.komamitsu.fluency.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.sender.heartbeat.Heartbeater;
import org.komamitsu.fluency.sender.heartbeat.TCPHeartbeater;
import org.komamitsu.fluency.util.Tuple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MultiSender
        implements Sender
{
    private final List<Tuple<TCPSender, FailureDetector>> sendersAndFailureDetectors = new ArrayList<Tuple<TCPSender, FailureDetector>>();

    public MultiSender(List<TCPSender> senders, FailureDetectStrategy failureDetectStrategy, Heartbeater.Factory heartbeaterFactory)
            throws IOException
    {
        // TODO: Fix bug sharing failureDetectStrategy
        for (TCPSender sender : senders) {
            Heartbeater.Config heartbeaterConfig = new Heartbeater.Config().setHost(sender.getHost()).setPort(sender.getPort());
            FailureDetector failureDetector = new FailureDetector(failureDetectStrategy, heartbeaterFactory, heartbeaterConfig);
            sendersAndFailureDetectors.add(new Tuple<TCPSender, FailureDetector>(sender, failureDetector));
        }
    }

    public MultiSender(List<TCPSender> senders, Heartbeater.Factory heartbeaterFactory)
            throws IOException
    {
        this(senders, new PhiAccrualFailureDetectStrategy(new PhiAccrualFailureDetectStrategy.Config()), heartbeaterFactory);
    }

    public MultiSender(List<TCPSender> senders)
            throws IOException
    {
        this(senders, new PhiAccrualFailureDetectStrategy(new PhiAccrualFailureDetectStrategy.Config()), new TCPHeartbeater.Factory());
    }

    public MultiSender(List<TCPSender> senders, FailureDetectStrategy failureDetectStrategy)
            throws IOException
    {
        this(senders, failureDetectStrategy, new TCPHeartbeater.Factory());
    }

    @Override
    public void send(ByteBuffer data)
            throws IOException
    {
        for (Tuple<TCPSender, FailureDetector> senderAndFailureDetector : sendersAndFailureDetectors) {
            TCPSender sender = senderAndFailureDetector.getFirst();
            FailureDetector failureDetector = senderAndFailureDetector.getSecond();
            if (failureDetector.isAvailable()) {
                sender.send(data);
                return;
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

