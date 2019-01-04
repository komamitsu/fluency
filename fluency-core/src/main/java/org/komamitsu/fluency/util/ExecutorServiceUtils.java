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
