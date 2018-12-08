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

package org.komamitsu.fluency.ingester.fluentdsender.retry;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConstantRetryStrategyTest
{
    @Test
    public void testGetNextIntervalMillis()
    {
        ConstantRetryStrategy.Config config = new ConstantRetryStrategy.Config().setRetryIntervalMillis(600).setMaxRetryCount(6);
        RetryStrategy strategy = config.createInstance();

        assertEquals(600, strategy.getNextIntervalMillis(0));
        assertEquals(600, strategy.getNextIntervalMillis(1));
        assertEquals(600, strategy.getNextIntervalMillis(2));
        assertEquals(600, strategy.getNextIntervalMillis(6));
        assertFalse(strategy.isRetriedOver(6));
        assertEquals(600, strategy.getNextIntervalMillis(7));
        assertTrue(strategy.isRetriedOver(7));
    }
}