/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency;

import org.komamitsu.fluency.buffer.Buffer;
import org.komamitsu.fluency.flusher.AsyncFlusher;
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.ingester.sender.ErrorHandler;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.RecordFormatter;

public class BaseFluencyBuilder
{
    public static Fluency buildFromConfigs(
            RecordFormatter.Instantiator recordFormatter,
            Buffer.Instantiator bufferConfig,
            Flusher.Instantiator flusherConfig,
            Ingester ingester)
    {
        Buffer buffer =
                (bufferConfig != null ? bufferConfig : new Buffer.Config()).
                        createInstance(recordFormatter.createInstance());

        Flusher flusher =
                (flusherConfig != null ? flusherConfig : new AsyncFlusher.Config()).
                        createInstance(buffer, ingester);

        return new Fluency(buffer, flusher);
    }

    public static class FluencyConfig
    {
        private Long maxBufferSize;

        private Integer bufferChunkInitialSize;

        private Integer bufferChunkRetentionSize;

        private Integer flushIntervalMillis;

        private String fileBackupDir;

        private Integer waitUntilBufferFlushed;

        private Integer waitUntilFlusherTerminated;

        private Boolean jvmHeapBufferMode;

        private ErrorHandler errorHandler;

        // TODO: Contain sender's retry setting here?

        public Long getMaxBufferSize()
        {
            return maxBufferSize;
        }

        public void setMaxBufferSize(Long maxBufferSize)
        {
            this.maxBufferSize = maxBufferSize;
        }

        public Integer getBufferChunkInitialSize()
        {
            return bufferChunkInitialSize;
        }

        public void setBufferChunkInitialSize(Integer bufferChunkInitialSize)
        {
            this.bufferChunkInitialSize = bufferChunkInitialSize;
        }

        public Integer getBufferChunkRetentionSize()
        {
            return bufferChunkRetentionSize;
        }

        public void setBufferChunkRetentionSize(Integer bufferChunkRetentionSize)
        {
            this.bufferChunkRetentionSize = bufferChunkRetentionSize;
        }

        public Integer getFlushIntervalMillis()
        {
            return flushIntervalMillis;
        }

        public void setFlushIntervalMillis(Integer flushIntervalMillis)
        {
            this.flushIntervalMillis = flushIntervalMillis;
        }

        public String getFileBackupDir()
        {
            return fileBackupDir;
        }

        public void setFileBackupDir(String fileBackupDir)
        {
            this.fileBackupDir = fileBackupDir;
        }

        public Integer getWaitUntilBufferFlushed()
        {
            return waitUntilBufferFlushed;
        }

        public void setWaitUntilBufferFlushed(Integer waitUntilBufferFlushed)
        {
            this.waitUntilBufferFlushed = waitUntilBufferFlushed;
        }

        public Integer getWaitUntilFlusherTerminated()
        {
            return waitUntilFlusherTerminated;
        }

        public void setWaitUntilFlusherTerminated(Integer waitUntilFlusherTerminated)
        {
            this.waitUntilFlusherTerminated = waitUntilFlusherTerminated;
        }

        public Boolean getJvmHeapBufferMode()
        {
            return jvmHeapBufferMode;
        }

        public void setJvmHeapBufferMode(Boolean jvmHeapBufferMode)
        {
            this.jvmHeapBufferMode = jvmHeapBufferMode;
        }

        public ErrorHandler getErrorHandler()
        {
            return errorHandler;
        }

        public void setErrorHandler(ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
        }

        @Override
        public String toString()
        {
            return "FluencyConfig{" +
                    "maxBufferSize=" + maxBufferSize +
                    ", bufferChunkInitialSize=" + bufferChunkInitialSize +
                    ", bufferChunkRetentionSize=" + bufferChunkRetentionSize +
                    ", flushIntervalMillis=" + flushIntervalMillis +
                    ", fileBackupDir='" + fileBackupDir + '\'' +
                    ", waitUntilBufferFlushed=" + waitUntilBufferFlushed +
                    ", waitUntilFlusherTerminated=" + waitUntilFlusherTerminated +
                    ", jvmHeapBufferMode=" + jvmHeapBufferMode +
                    ", errorHandler=" + errorHandler +
                    '}';
        }
    }

    public static class Configs {
        private final Buffer.Config bufferConfig;
        private final AsyncFlusher.Config flusherConfig;

        public Configs(Buffer.Config bufferConfig, AsyncFlusher.Config flusherConfig)
        {
            this.bufferConfig = bufferConfig;
            this.flusherConfig = flusherConfig;
        }

        public Buffer.Config getBufferConfig()
        {
            return bufferConfig;
        }

        public AsyncFlusher.Config getFlusherConfig()
        {
            return flusherConfig;
        }
    }

    public static Configs buildConfigs(FluencyConfig config)
    {
        Buffer.Config bufferConfig = new Buffer.Config();
        AsyncFlusher.Config flusherConfig = new AsyncFlusher.Config();

        if (config.getMaxBufferSize() != null) {
            bufferConfig.setMaxBufferSize(config.getMaxBufferSize());
        }

        if (config.getBufferChunkInitialSize() != null) {
            bufferConfig.setChunkInitialSize(config.getBufferChunkInitialSize());
        }

        if (config.getBufferChunkRetentionSize() != null) {
            bufferConfig.setChunkRetentionSize(config.getBufferChunkRetentionSize());
        }

        if (config.getFileBackupDir() != null) {
            bufferConfig.setFileBackupDir(config.getFileBackupDir());
        }

        if (config.getJvmHeapBufferMode() != null) {
            bufferConfig.setJvmHeapBufferMode(config.jvmHeapBufferMode);
        }

        if (config.getFlushIntervalMillis() != null) {
            flusherConfig.setFlushIntervalMillis(config.getFlushIntervalMillis());
        }

        if (config.getWaitUntilBufferFlushed() != null) {
            flusherConfig.setWaitUntilBufferFlushed(config.getWaitUntilBufferFlushed());
        }

        if (config.getWaitUntilFlusherTerminated() != null) {
            flusherConfig.setWaitUntilTerminated(config.getWaitUntilFlusherTerminated());
        }

        return new Configs(bufferConfig, flusherConfig);
    }
}
