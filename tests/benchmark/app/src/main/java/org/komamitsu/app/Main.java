/*
 * Copyright 2022 Mitsunori Komatsu (komamitsu)
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

package org.komamitsu.app;

import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.fluentd.FluencyBuilderForFluentd;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Main {
    private long runBench(Fluency fluency, String tag, int count) throws IOException {
        Map<String, Object> event = new HashMap<>();
        event.put("name", "komamitsu");
        event.put("comment", "zzz");
        long startInMillis = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            fluency.emit(tag, event);
        }
        fluency.flush();
        return System.currentTimeMillis() - startInMillis;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String tag = args[0];
        int count = Integer.parseInt(args[1]);

        FluencyBuilderForFluentd builder = new FluencyBuilderForFluentd();
        builder.setWaitUntilBufferFlushed(5000);
        builder.setWaitUntilFlusherTerminated(5000);
        builder.setSenderMaxRetryCount(8);
        try (Fluency fluency = builder.build()) {
            Main app = new Main();
            // Warmup
            app.runBench(fluency, tag, count);
            TimeUnit.SECONDS.sleep(10);
            long durationInMillis = app.runBench(fluency, tag, count);
            System.out.println("DURATION: " + durationInMillis);
        }
    }
}
