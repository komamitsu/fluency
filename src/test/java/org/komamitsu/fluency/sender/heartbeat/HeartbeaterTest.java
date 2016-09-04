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
    private TestableHeartbeater.Config config;

    private static class TestableHeartbeater extends Heartbeater
    {
        private List<Tuple3<Long, String, Integer>> pongedRecords = new ArrayList<Tuple3<Long, String, Integer>>();
        public TestableHeartbeater(Config config)
        {
            super(config);
        }

        public List<Tuple3<Long, String, Integer>> getPongedRecords()
        {
            return pongedRecords;
        }

        @Override
        protected void invoke()
        {
            pongedRecords.add(new Tuple3<Long, String, Integer>(System.currentTimeMillis(), config.getHost(), config.getPort()));
        }

        private static class Config extends Heartbeater.Config<Config>
        {
            @Override
            public Heartbeater createInstance()
                    throws IOException
            {
                return new TestableHeartbeater(this);
            }
        }
    }

    @Before
    public void setup()
    {
        config = new TestableHeartbeater.Config().setHost("dummy-hostname").setPort(123456).setIntervalMillis(300);
    }

    @Test
    public void testHeartbeater()
            throws InterruptedException, IOException
    {
        TestableHeartbeater heartbeater = null;
        try {
            heartbeater = new TestableHeartbeater(config);
            heartbeater.start();
            TimeUnit.SECONDS.sleep(1);
            heartbeater.close();
            TimeUnit.MILLISECONDS.sleep(500);
            assertTrue(1 < heartbeater.pongedRecords.size() && heartbeater.pongedRecords.size() < 4);
            Tuple3<Long, String, Integer> firstPong = heartbeater.pongedRecords.get(0);
            Tuple3<Long, String, Integer> secondPong = heartbeater.pongedRecords.get(1);
            long diff = secondPong.getFirst() - firstPong.getFirst();
            assertTrue(100 < diff && diff < 1000);
            for (Tuple3<Long, String, Integer> pongedRecord : heartbeater.pongedRecords) {
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