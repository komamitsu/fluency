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

package org.komamitsu.fluency.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileBackup
{
    private static final Logger LOG = LoggerFactory.getLogger(FileBackup.class);
    private static final String PARAM_DELIM_IN_FILENAME = "#";
    private static final String EXT_FILENAME = ".buf";
    private final File backupDir;
    private final Buffer userBuffer;
    private final Pattern pattern;
    private final String prefix;

    public static class SavedBuffer
        implements Closeable
    {
        private final List<String> params;
        private final File savedFile;
        private FileChannel channel;

        public SavedBuffer(File savedFile, List<String> params)
        {
            this.savedFile = savedFile;
            this.params = params;
        }

        public void open(Callback callback)
        {
            try {
                channel = new RandomAccessFile(savedFile, "rw").getChannel();
                callback.process(params, channel);
                success();
            }
            catch (Exception e) {
                LOG.error("Failed to process file. Skipping the file: file=" + savedFile, e);
            }
            finally {
                try {
                    close();
                }
                catch (IOException e) {
                    LOG.warn("Failed to close file: file=" + savedFile, e);
                }
            }
        }

        public void remove()
        {
            if (!savedFile.delete()) {
                LOG.warn("Failed to delete file: file=" + savedFile);
            }
        }

        private void success()
        {
            try {
                close();
            }
            catch (IOException e) {
                LOG.warn("Failed to close file: file=" + savedFile, e);
            }
            finally {
                remove();
            }
        }

        @Override
        public void close()
                throws IOException
        {
            if (channel != null && channel.isOpen()) {
                channel.close();
                channel = null;
            }
        }

        public interface Callback
        {
            void process(List<String> params, FileChannel channel);
        }
    }

    private String prefix()
    {
        return prefix == null ? "" : "_" + prefix;
    }

    public FileBackup(File backupDir, Buffer userBuffer, String prefix)
    {
        if (backupDir.mkdir()) {
            LOG.info("Created backupDir: dir={}", backupDir);
        }
        if (!backupDir.isDirectory() || !backupDir.canRead() || !backupDir.canWrite()) {
            throw new IllegalArgumentException("backupDir[" + backupDir + "] needs to be a readable & writable directory");
        }
        this.backupDir = backupDir;
        this.userBuffer = userBuffer;
        this.prefix = prefix;
        this.pattern = Pattern.compile(userBuffer.bufferFormatType() + prefix() + PARAM_DELIM_IN_FILENAME + "([\\w\\." + PARAM_DELIM_IN_FILENAME + "]+)" + EXT_FILENAME);
        LOG.debug(this.toString());
    }

    @Override
    public String toString()
    {
        return "FileBackup{" +
                "backupDir=" + backupDir +
                ", userBuffer=" + userBuffer +
                ", pattern=" + pattern +
                ", prefix='" + prefix + '\'' +
                '}';
    }

    public List<SavedBuffer> getSavedFiles()
    {
        File[] files = backupDir.listFiles();
        if (files == null) {
            LOG.warn("Failed to list the backup directory. {}", backupDir);
            return new ArrayList<>();
        }

        LOG.debug("Checking backup files. files.length={}", files.length);
        ArrayList<SavedBuffer> savedBuffers = new ArrayList<>();
        for (File f : files) {
            Matcher matcher = pattern.matcher(f.getName());
            if (matcher.find()) {
                if (matcher.groupCount() != 1) {
                    LOG.warn("Invalid backup filename: file={}", f.getName());
                }
                else {
                    String concatParams = matcher.group(1);
                    String[] params = concatParams.split(PARAM_DELIM_IN_FILENAME);
                    LinkedList<String> paramList = new LinkedList<>(Arrays.asList(params));
                    LOG.debug("Saved buffer params={}", paramList);
                    paramList.removeLast();
                    savedBuffers.add(new SavedBuffer(f, paramList));
                }
            }
            else {
                LOG.trace("Found a file in backup dir, but the file path doesn't match the pattern. file={}", f.getAbsolutePath());
            }
        }
        return savedBuffers;
    }

    public void saveBuffer(List<String> params, ByteBuffer buffer)
    {
        List<String> copiedParams = new ArrayList<>(params);
        copiedParams.add(String.valueOf(System.nanoTime()));

        boolean isFirst = true;
        StringBuilder sb = new StringBuilder();
        for (String param : copiedParams) {
            if (isFirst) {
                isFirst = false;
            }
            else {
                sb.append(PARAM_DELIM_IN_FILENAME);
            }
            sb.append(param);
        }
        String filename = this.userBuffer.bufferFormatType() + prefix() + PARAM_DELIM_IN_FILENAME + sb.toString() + EXT_FILENAME;

        File file = new File(backupDir, filename);
        LOG.debug("Backing up buffer: path={}, size={}", file.getAbsolutePath(), buffer.remaining());

        FileChannel channel = null;
        try {
            channel = new FileOutputStream(file).getChannel();
            channel.write(buffer);
        }
        catch (Exception e) {
            LOG.error("Failed to save buffer to file: params=" + copiedParams + ", path=" + file.getAbsolutePath() + ", buffer=" + buffer, e);
        }
        finally {
            if (channel != null) {
                try {
                    channel.close();
                }
                catch (IOException e) {
                    LOG.warn("Failed to close Channel: channel=" + channel);
                }
            }
        }
    }
}
