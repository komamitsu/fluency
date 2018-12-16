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

package org.komamitsu.fluency.integration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.FluencyBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assume.assumeNotNull;

public class WithRealFluentd
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    public static class EmitTask
            implements Callable<Void>
    {
        private final Fluency fluency;
        private final String tag;
        private final Map<String, Object> data;
        private final int count;

        private EmitTask(Fluency fluency, String tag, Map<String, Object> data, int count)
        {
            this.fluency = fluency;
            this.tag = tag;
            this.data = data;
            this.count = count;
        }

        @Override
        public Void call()
        {
            for (int i = 0; i < count; i++) {
                try {
                    fluency.emit(tag, data);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    try {
                        TimeUnit.MILLISECONDS.sleep(500);
                    }
                    catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }
            return null;
        }
    }

    public static class Config
    {
        @JsonProperty("host")
        public final String host;
        @JsonProperty("port")
        public final Integer port;
        @JsonProperty("another_host")
        public final String anotherHost;
        @JsonProperty("another_port")
        public final Integer anotherPort;
        @JsonProperty("tag")
        public final String tag;
        @JsonProperty("requests")
        public final int requests;
        @JsonProperty("concurrency")
        public final int concurrency;
        @JsonProperty("wait_seconds")
        public final int waitSeconds;
        @JsonProperty("ssl_enabled")
        public final boolean sslEnabled;

        public Config(
                @JsonProperty("host")
                        String host,
                @JsonProperty("port")
                        Integer port,
                @JsonProperty("another_host")
                        String anotherHost,
                @JsonProperty("another_port")
                        Integer anotherPort,
                @JsonProperty("tag")
                        String tag,
                @JsonProperty("requests")
                        Integer requests,
                @JsonProperty("concurrency")
                        Integer concurrency,
                @JsonProperty("wait_seconds")
                        Integer waitSeconds,
                @JsonProperty("ssl_enabled")
                        Boolean sslEnabled
                )
        {
            this.host = host == null ? "127.0.0.1" : host;
            this.port = port == null ? Integer.valueOf(24224) : port;

            this.anotherHost = anotherHost == null ? "127.0.0.1" : anotherHost;
            // Nullable
            this.anotherPort = anotherPort;

            this.tag = tag == null ? "foodb.bartbl" : tag;
            this.requests = requests == null ? 1000000 : requests;
            this.concurrency = concurrency == null ? 4 : concurrency;
            this.waitSeconds = waitSeconds == null ? 60 : waitSeconds;
            this.sslEnabled = sslEnabled == null ? false : sslEnabled;
        }
    }

    WithRealFluentd.Config getConfig()
            throws IOException
    {
        String conf = System.getenv("WITH_FLUENTD");
        if (conf == null) {
            return null;
        }

        return objectMapper.readValue(conf, WithRealFluentd.Config.class);
    }

    @Test
    public void testWithRealFluentd()
            throws Exception
    {
        WithRealFluentd.Config config = getConfig();
        assumeNotNull(config);

        Fluency fluency = FluencyBuilder.ForFluentd.build(
                config.host,
                config.port,
                new FluencyBuilder.ForFluentd.FluencyConfig()
                        .setSslEnabled(config.sslEnabled)
        );

        Map<String, Object> data = new HashMap<String, Object>();
        data.put("name", "komamitsu");
        data.put("age", 42);
        data.put("comment", "hello, world");
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Future<Void>> futures = new ArrayList<Future<Void>>();
        try {
            for (int i = 0; i < config.concurrency; i++) {
                futures.add(executorService.submit(new EmitTask(fluency, config.tag, data, config.requests)));
            }
            for (Future<Void> future : futures) {
                future.get(config.waitSeconds, TimeUnit.SECONDS);
            }
        }
        finally {
            fluency.close();
        }
    }

    @Test
    public void testWithRealMultipleFluentd()
            throws IOException, InterruptedException, TimeoutException, ExecutionException
    {
        WithRealFluentd.Config config = getConfig();
        assumeNotNull(config);
        assumeNotNull(config.anotherPort);

        Fluency fluency = FluencyBuilder.ForFluentd.build(
                Arrays.asList(
                        new InetSocketAddress(config.host, config.port),
                        new InetSocketAddress(config.host, config.anotherPort)),
                new FluencyBuilder.ForFluentd.FluencyConfig()
                        .setSslEnabled(config.sslEnabled)
                        .setAckResponseMode(true));

        Map<String, Object> data = new HashMap<>();
        data.put("name", "komamitsu");
        data.put("age", 42);
        data.put("comment", "hello, world");
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Future<Void>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < config.concurrency; i++) {
                futures.add(executorService.submit(new EmitTask(fluency, config.tag, data, config.requests)));
            }
            for (Future<Void> future : futures) {
                future.get(config.waitSeconds, TimeUnit.SECONDS);
            }
        }
        finally {
            fluency.close();
        }
    }

    @Test
    public void testWithRealFluentdWithFileBackup()
            throws ExecutionException, TimeoutException, IOException, InterruptedException
    {
        WithRealFluentd.Config config = getConfig();
        assumeNotNull(config);

        Fluency fluency = FluencyBuilder.ForFluentd.build(
                config.host,
                config.port,
                new FluencyBuilder.ForFluentd.FluencyConfig()
                        // Fluency might use a lot of buffer for loaded backup files.
                        // So it'd better increase max buffer size
                        .setSslEnabled(config.sslEnabled)
                        .setMaxBufferSize(512 * 1024 * 1024L)
                        .setFileBackupDir(System.getProperty("java.io.tmpdir")));

        Map<String, Object> data = new HashMap<>();
        data.put("name", "komamitsu");
        data.put("age", 42);
        data.put("comment", "hello, world");

        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Future<Void>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < config.concurrency; i++) {
                futures.add(executorService.submit(new EmitTask(fluency, config.tag, data, config.requests)));
            }
            for (Future<Void> future : futures) {
                future.get(config.waitSeconds, TimeUnit.SECONDS);
            }
        }
        finally {
            fluency.close();
        }
    }
}
