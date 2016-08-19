package org.komamitsu.fluency.buffer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.sender.Sender;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public abstract class Buffer
{
    private static final Logger LOG = LoggerFactory.getLogger(Buffer.class);
    protected static final Charset CHARSET = Charset.forName("ASCII");
    protected final Config bufferConfig;
    protected final ThreadLocal<ObjectMapper> objectMapperHolder = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue()
        {
            return new ObjectMapper(new MessagePackFactory());
        }
    };
    protected final ThreadLocal<ByteArrayOutputStream> outputStreamHolder = new ThreadLocal<ByteArrayOutputStream>() {
        @Override
        protected ByteArrayOutputStream initialValue()
        {
            return new ByteArrayOutputStream();
        }
    };
    protected final FileBackup fileBackup;

    public Buffer(Config bufferConfig)
    {
        this.bufferConfig = bufferConfig;
        if (bufferConfig.getFileBackupDir() != null) {
            fileBackup = new FileBackup(new File(bufferConfig.getFileBackupDir()), this, bufferConfig.getFileBackupPrefix());
        }
        else {
            fileBackup = null;
        }
    }

    protected Config getConfig()
    {
        return bufferConfig;
    }

    public void init()
    {
        if (fileBackup != null) {
            for (FileBackup.SavedBuffer savedBuffer : fileBackup.getSavedFiles()) {
                savedBuffer.open(new FileBackup.SavedBuffer.Callback() {
                    @Override
                    public void process(List<String> params, FileChannel channel)
                    {
                        LOG.info("Loading buffer: params={}, buffer={}", params, channel);
                        loadBufferFromFile(params, channel);
                    }
                });
            }
        }
    }

    public abstract void append(String tag, long timestamp, Map<String, Object> data)
            throws IOException;

    protected abstract void loadBufferFromFile(List<String> params, FileChannel channel);

    protected abstract void saveAllBuffersToFile()
            throws IOException;

    protected void saveBuffer(List<String> params, ByteBuffer buffer)
    {
        if (fileBackup == null) {
            return;
        }
        LOG.info("Saving buffer: params={}, buffer={}", params, buffer);
        fileBackup.saveBuffer(params, buffer);
    }

    public void flush(Sender sender, boolean force)
            throws IOException
    {
        LOG.trace("flush(): force={}, bufferUsage={}", force, getBufferUsage());
        flushInternal(sender, force);
    }

    protected abstract void flushInternal(Sender sender, boolean force)
            throws IOException;

    public abstract String bufferFormatType();

    public void close()
    {
        try {
            LOG.info("Saving all buffers");
            saveAllBuffersToFile();
        }
        catch (Exception e) {
            LOG.warn("Failed to save all buffers", e);
        }
        LOG.info("Closing buffers");
        closeInternal();
    }

    protected abstract void closeInternal();

    public abstract long getAllocatedSize();

    public long getMaxSize()
    {
        return bufferConfig.getMaxBufferSize();
    }

    public float getBufferUsage()
    {
        return (float) getAllocatedSize() / getMaxSize();
    }

    public abstract long getBufferedDataSize();

    public void clearBackupFiles()
    {
        if (fileBackup != null) {
            for (FileBackup.SavedBuffer buffer : fileBackup.getSavedFiles()) {
                buffer.remove();
            }
        }
    }

    public abstract static class Config<BufferImpl extends Buffer, ConfigImpl extends Config>
    {
        protected long maxBufferSize = 512 * 1024 * 1024;
        protected boolean ackResponseMode = false;
        protected String fileBackupDir;
        protected String fileBackupPrefix;  // Mainly for testing

        protected abstract ConfigImpl self();

        public long getMaxBufferSize()
        {
            return maxBufferSize;
        }

        public ConfigImpl setMaxBufferSize(long maxBufferSize)
        {
            this.maxBufferSize = maxBufferSize;
            return self();
        }

        public boolean isAckResponseMode()
        {
            return ackResponseMode;
        }

        public ConfigImpl setAckResponseMode(boolean ackResponseMode)
        {
            this.ackResponseMode = ackResponseMode;
            return self();
        }

        public String getFileBackupDir()
        {
            return fileBackupDir;
        }

        public ConfigImpl setFileBackupDir(String fileBackupDir)
        {
            this.fileBackupDir = fileBackupDir;
            return self();
        }

        public String getFileBackupPrefix()
        {
            return fileBackupPrefix;
        }

        public ConfigImpl setFileBackupPrefix(String fileBackupPrefix)
        {
            this.fileBackupPrefix = fileBackupPrefix;
            return self();
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "maxBufferSize=" + maxBufferSize +
                    ", ackResponseMode=" + ackResponseMode +
                    ", fileBackupDir='" + fileBackupDir + '\'' +
                    ", fileBackupPrefix='" + fileBackupPrefix +
                    '}';
        }

        protected abstract BufferImpl createInstanceInternal();

        public BufferImpl createInstance()
        {
            BufferImpl instance = createInstanceInternal();
            instance.init();
            return instance;
        }
    }
}
