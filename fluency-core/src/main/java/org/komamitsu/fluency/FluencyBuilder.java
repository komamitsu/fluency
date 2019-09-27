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
import org.komamitsu.fluency.buffer.DefaultBuffer;
import org.komamitsu.fluency.buffer.SyncBuffer;
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
    private Integer flushAttemptIntervalMillis;
    private String fileBackupDir;
    private Integer waitUntilBufferFlushed;
    private Integer waitUntilFlusherTerminated;
    private Boolean jvmHeapBufferMode;
    private Boolean useSyncBuffer = false;
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

    /**
     * @deprecated  As of release 2.4.0, replaced by {@link #getFlushAttemptIntervalMillis()}
     */
    @Deprecated
    public Integer getFlushIntervalMillis()
    {
        return flushAttemptIntervalMillis;
    }

    /**
     * @deprecated  As of release 2.4.0, replaced by {@link #setFlushAttemptIntervalMillis(Integer flushAttemptIntervalMillis)}
     */
    @Deprecated
    public void setFlushIntervalMillis(Integer flushAttemptIntervalMillis)
    {
        this.flushAttemptIntervalMillis = flushAttemptIntervalMillis;
    }

    public Integer getFlushAttemptIntervalMillis()
    {
        return flushAttemptIntervalMillis;
    }

    public void setFlushAttemptIntervalMillis(Integer flushAttemptIntervalMillis)
    {
        this.flushAttemptIntervalMillis = flushAttemptIntervalMillis;
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

    public Boolean getUseSyncBuffer()
    {
        return useSyncBuffer;
    }

    public void setUseSyncBuffer(Boolean useSyncBuffer)
    {
        this.useSyncBuffer = useSyncBuffer;
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
                ", flushAttemptIntervalMillis=" + flushAttemptIntervalMillis +
                ", fileBackupDir='" + fileBackupDir + '\'' +
                ", waitUntilBufferFlushed=" + waitUntilBufferFlushed +
                ", waitUntilFlusherTerminated=" + waitUntilFlusherTerminated +
                ", jvmHeapBufferMode=" + jvmHeapBufferMode +
                ", useSyncBuffer=" + useSyncBuffer +
                ", errorHandler=" + errorHandler +
                '}';
    }

    @VisibleForTesting
    public Fluency createFluency(
            Ingester ingester,
            Buffer buffer,
            Flusher.Config flusherConfig)
    {
        Flusher flusher = new Flusher(flusherConfig, buffer, ingester);
        return new Fluency(buffer, flusher);
    }

    public Fluency buildFromIngester(RecordFormatter recordFormatter, Ingester ingester)
    {
        Buffer buffer;
        if (useSyncBuffer == null || useSyncBuffer == false) {
            DefaultBuffer.Config bufferConfig = new DefaultBuffer.Config();
            configureBufferConfig(bufferConfig);
            buffer = new DefaultBuffer(bufferConfig, recordFormatter);
        } else {
            buffer = new SyncBuffer(ingester, recordFormatter);
        }

        Flusher.Config flusherConfig = new Flusher.Config();
        configureFlusherConfig(flusherConfig);

        return createFluency(ingester, buffer, flusherConfig);
    }

    private void configureBufferConfig(DefaultBuffer.Config bufferConfig)
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
        if (getFlushAttemptIntervalMillis() != null) {
            flusherConfig.setFlushAttemptIntervalMillis(getFlushAttemptIntervalMillis());
        }

        if (getWaitUntilBufferFlushed() != null) {
            flusherConfig.setWaitUntilBufferFlushed(getWaitUntilBufferFlushed());
        }

        if (getWaitUntilFlusherTerminated() != null) {
            flusherConfig.setWaitUntilTerminated(getWaitUntilFlusherTerminated());
        }
    }
}
