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

public abstract class BaseFluencyBuilder
{
    private Long maxBufferSize;
    private Integer bufferChunkInitialSize;
    private Integer bufferChunkRetentionSize;
    private Integer bufferChunkRetentionTimeMillis;
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

    public Integer getBufferChunkRetentionTimeMillis()
    {
        return bufferChunkRetentionTimeMillis;
    }

    public void setBufferChunkRetentionTimeMillis(Integer bufferChunkRetentionTimeMillis)
    {
        this.bufferChunkRetentionTimeMillis = bufferChunkRetentionTimeMillis;
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
        return "BaseFluencyBuilder{" +
                "maxBufferSize=" + maxBufferSize +
                ", bufferChunkInitialSize=" + bufferChunkInitialSize +
                ", bufferChunkRetentionSize=" + bufferChunkRetentionSize +
                ", bufferChunkRetentionTimeMillis=" + bufferChunkRetentionTimeMillis +
                ", flushIntervalMillis=" + flushIntervalMillis +
                ", fileBackupDir='" + fileBackupDir + '\'' +
                ", waitUntilBufferFlushed=" + waitUntilBufferFlushed +
                ", waitUntilFlusherTerminated=" + waitUntilFlusherTerminated +
                ", jvmHeapBufferMode=" + jvmHeapBufferMode +
                ", errorHandler=" + errorHandler +
                '}';
    }

    public Fluency buildFromConfigs(
            RecordFormatter.Instantiator recordFormatterConfig,
            Buffer.Instantiator bufferConfig,
            Flusher.Instantiator flusherConfig,
            Ingester ingester)
    {
        Buffer buffer =
                (bufferConfig != null ? bufferConfig : new Buffer.Config()).
                        createInstance(recordFormatterConfig.createInstance());

        Flusher flusher =
                (flusherConfig != null ? flusherConfig : new AsyncFlusher.Config()).
                        createInstance(buffer, ingester);

        return new Fluency(buffer, flusher);
    }

    public static class Configs
    {
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

    protected Configs buildConfigs()
    {
        Buffer.Config bufferConfig = new Buffer.Config();
        AsyncFlusher.Config flusherConfig = new AsyncFlusher.Config();

        if (getMaxBufferSize() != null) {
            bufferConfig.setMaxBufferSize(getMaxBufferSize());
        }

        if (getBufferChunkInitialSize() != null) {
            bufferConfig.setChunkInitialSize(getBufferChunkInitialSize());
        }

        if (getBufferChunkRetentionSize() != null) {
            bufferConfig.setChunkRetentionSize(getBufferChunkRetentionSize());
        }

        if (getBufferChunkRetentionTimeMillis() != null) {
            bufferConfig.setChunkRetentionTimeMillis(getBufferChunkRetentionTimeMillis());
        }

        if (getFileBackupDir() != null) {
            bufferConfig.setFileBackupDir(getFileBackupDir());
        }

        if (getJvmHeapBufferMode() != null) {
            bufferConfig.setJvmHeapBufferMode(getJvmHeapBufferMode());
        }

        if (getFlushIntervalMillis() != null) {
            flusherConfig.setFlushIntervalMillis(getFlushIntervalMillis());
        }

        if (getWaitUntilBufferFlushed() != null) {
            flusherConfig.setWaitUntilBufferFlushed(getWaitUntilBufferFlushed());
        }

        if (getWaitUntilFlusherTerminated() != null) {
            flusherConfig.setWaitUntilTerminated(getWaitUntilFlusherTerminated());
        }

        return new Configs(bufferConfig, flusherConfig);
    }
}
