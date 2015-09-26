package org.komamitsu.fluency.sender;

import org.junit.Test;
import org.komamitsu.fluency.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TCPSenderTest
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPSenderTest.class);
    private static class ServerTask
            implements Runnable
    {
        private final List<Tuple<Type, Integer>> events = new CopyOnWriteArrayList<Tuple<Type, Integer>>();
        private final ExecutorService executorService;
        private final ServerSocketChannel serverSocketChannel;

        public enum Type
        {
            CONNECT, RECV, CLOSE;
        }

        private ServerTask(ExecutorService executorService)
                throws IOException
        {
            this.executorService = executorService;
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(null);
        }

        public int getLocalPort()
        {
            return serverSocketChannel.socket().getLocalPort();
        }

        public List<Tuple<Type, Integer>> getEvents()
        {
            return events;
        }

        private static class AcceptTask
                implements Runnable
        {
            private final List<Tuple<Type, Integer>> events;
            private final SocketChannel accept;

            private AcceptTask(List<Tuple<Type, Integer>> events, SocketChannel accept)
            {
                this.events = events;
                this.accept = accept;
            }

            @Override
            public void run()
            {
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
                while (accept.isOpen()) {
                    try {
                        byteBuffer.clear();
                        int len = accept.read(byteBuffer);
                        if (len < 0) {
                            LOG.debug("AcceptTask: closed");
                            events.add(new Tuple<Type, Integer>(Type.CLOSE, null));
                            try {
                                accept.close();
                            }
                            catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        else {
                            events.add(new Tuple<Type, Integer>(Type.RECV, len));
                        }
                    }
                    catch (ClosedChannelException e) {
                        LOG.debug("AcceptTask: channel is closed");
                        events.add(new Tuple<Type, Integer>(Type.CLOSE, null));
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void run()
        {
            while (!executorService.isShutdown()) {
                Thread acceptThread = null;
                SocketChannel accept = null;
                try {
                    accept = serverSocketChannel.accept();
                    events.add(new Tuple<Type, Integer>(Type.CONNECT, null));
                    acceptThread = new Thread(new AcceptTask(events, accept));
                    acceptThread.start();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                finally {
                    if (acceptThread != null) {
                        try {
                            acceptThread.join();
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            try {
                serverSocketChannel.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testSend()
            throws IOException, InterruptedException
    {
        final ExecutorService serverExecutorService = Executors.newSingleThreadExecutor();

        ServerTask serverTask = new ServerTask(serverExecutorService);
        serverExecutorService.execute(serverTask);

        int concurency = 20;
        final int reqNum = 2000;
        final CountDownLatch latch = new CountDownLatch(concurency);
        final TCPSender sender = new TCPSender(serverTask.getLocalPort());
        final ExecutorService senderExecutorService = Executors.newCachedThreadPool();
        for (int i = 0; i < concurency; i++) {
            senderExecutorService.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        byte[] bytes = "0123456789".getBytes();

                        for (int j = 0; j < reqNum; j++) {
                            sender.send(ByteBuffer.wrap(bytes));
                        }
                        latch.countDown();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
        sender.close();
        TimeUnit.MILLISECONDS.sleep(500);

        serverExecutorService.shutdownNow();
        serverExecutorService.awaitTermination(1, TimeUnit.SECONDS);

        int connectCount = 0;
        int closeCount = 0;
        long recvCount = 0;
        long recvLen = 0;
        for (Tuple<ServerTask.Type, Integer> event : serverTask.getEvents()) {
            switch (event.getFirst()) {
                case CONNECT:
                    connectCount++;
                    break;
                case CLOSE:
                    closeCount++;
                    break;
                case RECV:
                    recvCount++;
                    recvLen += event.getSecond();
                    break;
            }
        }
        LOG.debug("recvCount={}", recvCount);

        assertEquals(1, connectCount);
        assertEquals(concurency * reqNum * 10, recvLen);
        assertEquals(1, closeCount);
    }
}