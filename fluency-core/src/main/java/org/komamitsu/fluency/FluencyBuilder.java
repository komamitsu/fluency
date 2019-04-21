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
import org.komamitsu.fluency.flusher.Flusher;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.ingester.sender.ErrorHandler;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.msgpack.core.annotations.VisibleForTesting;

public class FluencyBuilder
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

    @VisibleForTesting
    public Fluency createFluency(
            RecordFormatter recordFormatter,
            Ingester ingester,
            Buffer.Config bufferConfig,
            Flusher.Config flusherConfig)
    {
        Buffer buffer = new Buffer(bufferConfig, recordFormatter);
        Flusher flusher = new Flusher(flusherConfig, buffer, ingester);
        return new Fluency(buffer, flusher);
    }

    public Fluency buildFromIngester(RecordFormatter recordFormatter, Ingester ingester)
    {
        Buffer.Config bufferConfig = new Buffer.Config();
        configureBufferConfig(bufferConfig);

        Flusher.Config flusherConfig = new Flusher.Config();
        configureFlusherConfig(flusherConfig);

        return createFluency(recordFormatter, ingester, bufferConfig, flusherConfig);
    }

    private void configureBufferConfig(Buffer.Config bufferConfig)
    {
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
    }

    protected void configureFlusherConfig(Flusher.Config flusherConfig)
    {
        if (getFlushIntervalMillis() != null) {
            flusherConfig.setFlushIntervalMillis(getFlushIntervalMillis());
        }

        if (getWaitUntilBufferFlushed() != null) {
            flusherConfig.setWaitUntilBufferFlushed(getWaitUntilBufferFlushed());
        }

        if (getWaitUntilFlusherTerminated() != null) {
            flusherConfig.setWaitUntilTerminated(getWaitUntilFlusherTerminated());
        }
    }
}
