package org.komamitsu.fluency.sender.failuredetect;

import org.junit.Test;
import org.komamitsu.failuredetector.PhiAccuralFailureDetector;
import org.komamitsu.fluency.sender.heartbeat.TCPHeartbeater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class FailureDetectorTest
{
    private static final Logger LOG = LoggerFactory.getLogger(FailureDetectorTest.class);

    @Test
    public void testIsAvailable()
            throws IOException, InterruptedException
    {
        final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(null);
        int localPort = serverSocketChannel.socket().getLocalPort();

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        Runnable serverTask = new Runnable()
        {
            @Override
            public void run()
            {
                while (!executorService.isShutdown()) {
                    SocketChannel accept = null;
                    try {
                        accept = serverSocketChannel.accept();
                        LOG.debug("Accepted: {}", accept);
                    }
                    catch (IOException e) {
                        LOG.warn("Stab TCP server got an error", e);
                    }
                    finally {
                        if (accept != null) {
                            try {
                                accept.close();
                            }
                            catch (IOException e) {
                            }
                        }
                    }
                }
            }
        };

        executorService.execute(serverTask);

        TCPHeartbeater.Config heartbeaterConfig = new TCPHeartbeater.Config();
        heartbeaterConfig.setPort(localPort);

        PhiAccrualFailureDetectStrategy.Config failureDetectorConfig = new PhiAccrualFailureDetectStrategy.Config();
        FailureDetector failureDetector = null;
        try {
            failureDetector = new FailureDetector(failureDetectorConfig, heartbeaterConfig);

            assertTrue(failureDetector.isAvailable());
            TimeUnit.SECONDS.sleep(2L);
            assertTrue(failureDetector.isAvailable());

            executorService.shutdownNow();
            TimeUnit.SECONDS.sleep(2L);
            assertFalse(failureDetector.isAvailable());
        }
        finally {
            failureDetector.close();
        }
    }
}