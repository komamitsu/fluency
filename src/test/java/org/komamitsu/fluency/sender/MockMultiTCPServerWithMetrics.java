package org.komamitsu.fluency.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class MockMultiTCPServerWithMetrics
        extends MockTCPServerWithMetrics
{
    private static final Logger LOG = LoggerFactory.getLogger(MockMultiTCPServerWithMetrics.class);
    private ExecutorService executorService;
    private DatagramChannel channel;

    public MockMultiTCPServerWithMetrics(boolean useSsl)
            throws IOException
    {
        super(useSsl);
    }

    @Override
    public synchronized void start()
            throws Exception
    {
        super.start();

        if (executorService != null) {
            return;
        }

        executorService = Executors.newSingleThreadExecutor();
        channel = DatagramChannel.open();
        channel.socket().bind(new InetSocketAddress(getLocalPort()));

        executorService.execute(new Runnable()
        {
            @Override
            public void run()
            {
                while (executorService != null && !executorService.isTerminated()) {
                    try {
                        ByteBuffer buffer = ByteBuffer.allocate(8);
                        SocketAddress socketAddress = channel.receive(buffer);
                        assertEquals(0, buffer.position());
                        channel.send(buffer, socketAddress);
                    }
                    catch (ClosedByInterruptException e) {
                        // Expected
                    }
                    catch (IOException e) {
                        LOG.warn("Failed to receive or send heartbeat", e);
                    }
                }
            }
        });
    }

    @Override
    public synchronized void stop()
            throws IOException
    {
        super.stop();

        if (executorService == null) {
            return;
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(2, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOG.warn("stop(): Interrupted while shutting down", e);
        }
        executorService.shutdownNow();
        executorService = null;
    }
}
