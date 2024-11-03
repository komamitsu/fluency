package org.komamitsu.fluency.buffer;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class FileBackupTest {

  private void createTempFile(File dir, String filename, String content) throws IOException {
    Path tempfilePath = Files.createFile(dir.toPath().resolve(filename));
    Files.write(tempfilePath, content.getBytes(StandardCharsets.UTF_8));
    tempfilePath.toFile().deleteOnExit();
  }

  private void assertSavedBuffer(FileBackup.SavedBuffer savedBuffer, Path expectedPath, byte[] expectedContent, String ... expectedParams) {
    assertThat(savedBuffer.getPath()).isEqualTo(expectedPath);
    savedBuffer.open((params, channel) -> {
      // FIXME
      assertThat(params).isEqualTo(Arrays.stream(expectedParams).limit(expectedParams.length - 1).collect(Collectors.toList()));
      try {
        long size = channel.size();
        ByteBuffer buf = ByteBuffer.allocate((int) size);
        channel.read(buf);
        assertThat(buf.array()).isEqualTo(expectedContent);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  void getSavedFiles_GivenEmptyFiles_ShouldReturnEmpty() throws IOException {
    File backupDir = Files.createTempDirectory("test").toFile();
    Buffer buffer = mock(Buffer.class);
    String prefix = "my_prefix";
    FileBackup fileBackup = new FileBackup(backupDir, buffer, prefix);

    assertThat(fileBackup.getSavedFiles()).isEmpty();
  }

  @Test
  void getSavedFiles_GivenSomeFiles_ShouldReturnThem() throws IOException {
    long nanoSeconds1 = System.nanoTime();
    long nanoSeconds2 = System.nanoTime();
    long nanoSeconds3 = System.nanoTime();
    File backupDir = Files.createTempDirectory("test").toFile();
    backupDir.deleteOnExit();
    createTempFile(backupDir,
        String.format("xmy_buf_type_my_prefix#%d#param_a#param_b.buf", System.nanoTime()), "ignored");
    createTempFile(backupDir,
        String.format("xmy_buf_type_my_prefix#%d#param_a#param_b.buf", System.nanoTime()), "ignored");
    createTempFile(backupDir,
        String.format("y_buf_type_my_prefix#%d#param_a#param_b.buf", System.nanoTime()), "ignored");
    createTempFile(backupDir,
        String.format("my_buf_type_my_prefix#%d#paramA#paramB.buf", nanoSeconds1), "content1");
    createTempFile(backupDir,
        String.format("my_buf_type_my_prefix#%d#param-a#param-b.buf", nanoSeconds2), "content2");
    createTempFile(backupDir,
        String.format("my_buf_type_my_prefix#%d#param_a#param_b.buf", nanoSeconds3), "content3");
    createTempFile(backupDir,
        String.format("my_buf_type_my_prefixz#%d#param_a#param_b.buf", System.nanoTime()), "ignored");
    createTempFile(backupDir,
        String.format("my_buf_type_my_prefi#%d#param_a#param_b.buf", System.nanoTime()), "ignored");
    createTempFile(backupDir,
        String.format("my_buf_type_my_prefix#%d#param:a#param:b.buf", System.nanoTime()), "ignored");
    createTempFile(backupDir,
        String.format("my_buf_type_my_prefix#%d#param_a#param_b", System.nanoTime()), "ignored");
    createTempFile(backupDir,
        String.format("my_buf_type_my_prefix#%d#param_a#param_b.buff", System.nanoTime()), "ignored");
    Buffer buffer = mock(Buffer.class);
    doReturn("my_buf_type").when(buffer).bufferFormatType();
    String prefix = "my_prefix";
    FileBackup fileBackup = new FileBackup(backupDir, buffer, prefix);

    List<FileBackup.SavedBuffer> savedFiles = fileBackup.getSavedFiles().stream().sorted(
        Comparator.comparing(FileBackup.SavedBuffer::getPath)).collect(Collectors.toList());
    System.out.println(savedFiles);
    assertThat(savedFiles).size().isEqualTo(3);

    assertSavedBuffer(savedFiles.get(0),
        backupDir.toPath().resolve(String.format("my_buf_type_my_prefix#%d#paramA#paramB.buf", nanoSeconds1)),
        "content1".getBytes(StandardCharsets.UTF_8),
        String.valueOf(nanoSeconds1),
        "paramA",
        "paramB");

    assertSavedBuffer(savedFiles.get(1),
        backupDir.toPath().resolve(String.format("my_buf_type_my_prefix#%d#param-a#param-b.buf", nanoSeconds2)),
        "content2".getBytes(StandardCharsets.UTF_8),
        String.valueOf(nanoSeconds2),
        "param-a",
        "param-b");

    assertSavedBuffer(savedFiles.get(2),
        backupDir.toPath().resolve(String.format("my_buf_type_my_prefix#%d#param_a#param_b.buf", nanoSeconds3)),
        "content3".getBytes(StandardCharsets.UTF_8),
        String.valueOf(nanoSeconds3),
        "param_a",
        "param_b");
  }
}