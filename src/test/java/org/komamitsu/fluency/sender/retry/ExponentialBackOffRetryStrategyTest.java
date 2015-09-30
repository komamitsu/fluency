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