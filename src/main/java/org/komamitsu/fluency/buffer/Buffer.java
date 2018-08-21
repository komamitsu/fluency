package org.komamitsu.fluency.buffer;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.komamitsu.fluency.EventTime;
import org.komamitsu.fluency.sender.Sender;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class Buffer
{
    private static final Logger LOG = LoggerFactory.getLogger(Buffer.class);
    protected static final Charset CHARSET = Charset.forName("ASCII");
    protected final ObjectMapper objectMapper;
    protected final FileBackup fileBackup;
    private final Config config;

    protected Buffer(final Config config)
    {
        this.config = config;
        if (config.getFileBackupDir() != null) {
            fileBackup = new FileBackup(new File(config.getFileBackupDir()), this, config.getFileBackupPrefix());
        }
        else {
            fileBackup = null;
        }

        objectMapper = new ObjectMapper(new MessagePackFactory());
        List<Module> jacksonModules = config.getJacksonModules();
        for (Module module : jacksonModules) {
            objectMapper.registerModule(module);
        }
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

    public abstract void append(String tag, EventTime timestamp, Map<String, Object> data)
            throws IOException;

    public abstract void appendMessagePackMapValue(String tag, long timestamp, byte[] mapValue, int offset, int len)
            throws IOException;

    public abstract void appendMessagePackMapValue(String tag, EventTime timestamp, byte[] mapValue, int offset, int len)
            throws IOException;

    public abstract void appendMessagePackMapValue(String tag, long timestamp, ByteBuffer mapValue)
            throws IOException;

    public abstract void appendMessagePackMapValue(String tag, EventTime timestamp, ByteBuffer mapValue)
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
            LOG.debug("Saving all buffers");
            saveAllBuffersToFile();
        }
        catch (Exception e) {
            LOG.warn("Failed to save all buffers", e);
        }
        LOG.debug("Closing buffers");
        closeInternal();
    }

    protected abstract void closeInternal();

    public abstract long getAllocatedSize();

    public long getMaxSize()
    {
        return config.getMaxBufferSize();
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

    public long getMaxBufferSize()
    {
        return config.getMaxBufferSize();
    }

    public boolean isAckResponseMode()
    {
        return config.isAckResponseMode();
    }

    public String getFileBackupPrefix()
    {
        return config.getFileBackupPrefix();
    }

    public String getFileBackupDir()
    {
        return config.getFileBackupDir();
    }

    public List<Module> getJacksonModules()
    {
        return Collections.unmodifiableList(config.getJacksonModules());
    }

    @Override
    public String toString()
    {
        return "Buffer{" +
                "objectMapper=" + objectMapper +
                ", fileBackup=" + fileBackup +
                ", config=" + config +
                '}';
    }

    public static class Config
    {
        protected long maxBufferSize = 512 * 1024 * 1024;
        protected boolean ackResponseMode = false;
        protected String fileBackupDir;
        protected String fileBackupPrefix;  // Mainly for testing
        protected List<Module> jacksonModules = Collections.emptyList();

        public long getMaxBufferSize()
        {
            return maxBufferSize;
        }

        public Config setMaxBufferSize(long maxBufferSize)
        {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public boolean isAckResponseMode()
        {
            return ackResponseMode;
        }

        public Config setAckResponseMode(boolean ackResponseMode)
        {
            this.ackResponseMode = ackResponseMode;
            return this;
        }

        public String getFileBackupDir()
        {
            return fileBackupDir;
        }

        public Config setFileBackupDir(String fileBackupDir)
        {
            this.fileBackupDir = fileBackupDir;
            return this;
        }

        public String getFileBackupPrefix()
        {
            return fileBackupPrefix;
        }

        public Config setFileBackupPrefix(String fileBackupPrefix)
        {
            this.fileBackupPrefix = fileBackupPrefix;
            return this;
        }


        public List<Module> getJacksonModules()
        {
            return jacksonModules;
        }

        public Config setJacksonModules(List<Module> jacksonModules)
        {
            this.jacksonModules = jacksonModules;
            return this;
        }

        @Override
        public String toString()
        {
            return "Config{" +
                    "maxBufferSize=" + maxBufferSize +
                    ", ackResponseMode=" + ackResponseMode +
                    ", fileBackupDir='" + fileBackupDir + '\'' +
                    ", fileBackupPrefix='" + fileBackupPrefix + '\'' +
                    ", jacksonModules=" + jacksonModules +
                    '}';
        }
    }

    public interface Instantiator
    {
        Buffer createInstance();
    }
}
