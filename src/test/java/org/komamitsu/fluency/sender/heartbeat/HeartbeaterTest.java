package org.komamitsu.fluency.sender.heartbeat;

import org.junit.Before;
import org.junit.Test;
import org.komamitsu.fluency.util.Tuple3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class HeartbeaterTest
{
    private List<Tuple3<Long, String, Integer>> pongedRecords;
    private Heartbeater.Config config;

    private class TestableHeartbeater extends Heartbeater
    {
        public TestableHeartbeater(Config config)
        {
            super(config);
        }

        @Override
        protected void ping()
        {
            pongedRecords.add(new Tuple3<Long, String, Integer>(System.currentTimeMillis(), config.getHost(), config.getPort()));
        }
    }

    @Before
    public void setup()
    {
        pongedRecords = new ArrayList<Tuple3<Long, String, Integer>>();
        config = new Heartbeater.Config().setHost("dummy-hostname").setPort(123456).setIntervalMillis(300);
    }

    @Test
    public void testHeartbeater()
            throws InterruptedException, IOException
    {
        TestableHeartbeater heartbeater = null;
        try {
            heartbeater = new TestableHeartbeater(config);
            TimeUnit.SECONDS.sleep(1);
            heartbeater.close();
            TimeUnit.MILLISECONDS.sleep(500);
            assertTrue(1 < pongedRecords.size() && pongedRecords.size() < 4);
            Tuple3<Long, String, Integer> firstPong = pongedRecords.get(0);
            Tuple3<Long, String, Integer> secondPong = pongedRecords.get(1);
            long diff = secondPong.getFirst() - firstPong.getFirst();
            assertTrue(100 < diff && diff < 1000);
            for (Tuple3<Long, String, Integer> pongedRecord : pongedRecords) {
                assertEquals("dummy-hostname", pongedRecord.getSecond());
                assertEquals((Integer) 123456, pongedRecord.getThird());
            }
        }
        finally {
            if (heartbeater != null) {
                heartbeater.close();
            }
        }
    }
}