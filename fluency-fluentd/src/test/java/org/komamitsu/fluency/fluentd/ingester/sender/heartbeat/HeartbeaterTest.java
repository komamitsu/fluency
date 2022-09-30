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

package org.komamitsu.fluency.fluentd.ingester.sender.heartbeat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.util.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HeartbeaterTest
{
    private TestableHeartbeater.Config config;

    @BeforeEach
    public void setup()
    {
        config = new TestableHeartbeater.Config();
        config.setHost("dummy-hostname");
        config.setPort(123456);
        config.setIntervalMillis(300);
    }

    @Test
    public void testHeartbeater()
            throws InterruptedException
    {
        try (TestableHeartbeater heartbeater = new TestableHeartbeater(config)) {
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
    }

    @Test
    void validateConfig()
    {
        TestableHeartbeater.Config invalidConfig = new TestableHeartbeater.Config();
        invalidConfig.setIntervalMillis(99);
        assertThrows(IllegalArgumentException.class, () -> new TestableHeartbeater(invalidConfig));
    }

    private static class TestableHeartbeater
            extends InetSocketHeartbeater
    {
        private final Config config;
        private List<Tuple3<Long, String, Integer>> pongedRecords = new ArrayList<>();

        public TestableHeartbeater()
        {
            this(new Config());
        }

        public TestableHeartbeater(Config config)
        {
            super(config);
            this.config = config;
        }

        public List<Tuple3<Long, String, Integer>> getPongedRecords()
        {
            return pongedRecords;
        }

        @Override
        protected void invoke()
        {
            pongedRecords.add(new Tuple3<>(System.currentTimeMillis(), config.getHost(), config.getPort()));
        }

        private static class Config
                extends InetSocketHeartbeater.Config
        {
        }
    }
}