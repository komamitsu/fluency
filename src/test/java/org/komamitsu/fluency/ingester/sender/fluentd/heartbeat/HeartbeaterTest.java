/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.ingester.sender.fluentd.heartbeat;

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
        private final Config config;
        private List<Tuple3<Long, String, Integer>> pongedRecords = new ArrayList<Tuple3<Long, String, Integer>>();

        public TestableHeartbeater(Config config)
        {
            super(config.getBaseConfig());
            this.config = config;
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

        private static class Config
            implements Instantiator
        {
            private final Heartbeater.Config baseConfig = new Heartbeater.Config();

            public Heartbeater.Config getBaseConfig()
            {
                return baseConfig;
            }

            public String getHost()
            {
                return baseConfig.getHost();
            }

            public int getPort()
            {
                return baseConfig.getPort();
            }

            public Config setIntervalMillis(int intervalMillis)
            {
                baseConfig.setIntervalMillis(intervalMillis);
                return this;
            }

            public int getIntervalMillis()
            {
                return baseConfig.getIntervalMillis();
            }

            public Config setHost(String host)
            {
                baseConfig.setHost(host);
                return this;
            }

            public Config setPort(int port)
            {
                baseConfig.setPort(port);
                return this;
            }

            @Override
            public TestableHeartbeater createInstance()
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