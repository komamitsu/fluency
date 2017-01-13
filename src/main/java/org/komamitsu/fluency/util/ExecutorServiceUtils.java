package org.komamitsu.fluency.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceUtils
{
    private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceUtils.class);

    public static void finishExecutorService(ExecutorService executorService)
    {
        finishExecutorService(executorService, 3);
    }

    public static void finishExecutorService(ExecutorService executorService, long waitSecond)
    {
        executorService.shutdown();
        try {
            executorService.awaitTermination(waitSecond, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOG.warn("1st awaitTermination was interrupted", e);
            Thread.currentThread().interrupt();
        }
        if (!executorService.isTerminated()) {
            executorService.shutdownNow();
        }
    }
}
