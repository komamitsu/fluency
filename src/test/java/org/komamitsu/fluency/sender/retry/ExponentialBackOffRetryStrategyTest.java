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

package org.komamitsu.fluency.sender.retry;

import org.junit.Test;

import static org.junit.Assert.*;

public class ExponentialBackOffRetryStrategyTest
{
    @Test
    public void testGetNextIntervalMillis()
    {
        ExponentialBackOffRetryStrategy.Config config = new ExponentialBackOffRetryStrategy.Config().setBaseIntervalMillis(400).setMaxIntervalMillis(30000).setMaxRetryCount(7);
        RetryStrategy strategy = config.createInstance();

        assertEquals(400, strategy.getNextIntervalMillis(0));
        assertEquals(800, strategy.getNextIntervalMillis(1));
        assertEquals(1600, strategy.getNextIntervalMillis(2));
        assertEquals(3200, strategy.getNextIntervalMillis(3));
        assertEquals(25600, strategy.getNextIntervalMillis(6));
        assertEquals(30000, strategy.getNextIntervalMillis(7));
        assertFalse(strategy.isRetriedOver(7));
        assertEquals(30000, strategy.getNextIntervalMillis(8));
        assertTrue(strategy.isRetriedOver(8));
    }
}