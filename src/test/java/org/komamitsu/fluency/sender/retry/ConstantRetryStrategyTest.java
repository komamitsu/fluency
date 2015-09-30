package org.komamitsu.fluency.sender.retry;

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