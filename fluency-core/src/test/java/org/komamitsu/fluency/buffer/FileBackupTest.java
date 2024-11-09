package org.komamitsu.fluency.buffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class FileBackupTest {

  private void createTempFile(File dir, String filename, String content) throws IOException {
    Path tempfilePath = Files.createFile(dir.toPath().resolve(filename));
    Files.write(tempfilePath, content.getBytes(StandardCharsets.UTF_8));
    tempfilePath.toFile().deleteOnExit();
  }

  private void assertSavedBuffer(
      FileBackup.SavedBuffer savedBuffer,
      Path expectedPath,
      byte[] expectedContent,
      String... expectedParams) {
    assertThat(savedBuffer.getPath()).isEqualTo(expectedPath);
    savedBuffer.open(
        (params, channel) -> {
          assertThat(params.toArray()).isEqualTo(expectedParams);
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

  private void assertSavedFile(
      File savedFile,
      String bufferFormatType,
      String prefix,
      long startNanos,
      long endNanos,
      String param1,
      String param2,
      byte[] expectedContent)
      throws IOException {
    String fileName = savedFile.toPath().getFileName().toString();
    assertThat(fileName).endsWith(".buf");

    String[] partsOfPath = fileName.substring(0, fileName.length() - ".buf".length()).split("#");
    assertThat(partsOfPath).hasSize(4);
    assertThat(partsOfPath[0]).isEqualTo(bufferFormatType + "_" + prefix);
    assertThat(partsOfPath[1]).isEqualTo(param1);
    assertThat(partsOfPath[2]).isEqualTo(param2);
    assertThat(Long.valueOf(partsOfPath[3])).isBetween(startNanos, endNanos);
    assertThat(Files.readAllBytes(savedFile.toPath())).isEqualTo(expectedContent);
  }

  @Test
  void getSavedFiles_GivenEmptyFiles_ShouldReturnEmpty() throws IOException {
    File backupDir = Files.createTempDirectory("test").toFile();
    backupDir.deleteOnExit();
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
    createTempFile(
        backupDir,
        String.format("xmy_buf_type_my_prefix#param_a#param_b#%d.buf", System.nanoTime()),
        "ignored");
    createTempFile(
        backupDir,
        String.format("xmy_buf_type_my_prefix#param_a#param_b#%d.buf", System.nanoTime()),
        "ignored");
    createTempFile(
        backupDir,
        String.format("y_buf_type_my_prefix#param_a#param_b#%d.buf", System.nanoTime()),
        "ignored");
    createTempFile(
        backupDir,
        String.format("my_buf_type_my_prefix#1paramA#1paramB#%d.buf", nanoSeconds1),
        "content1");
    createTempFile(
        backupDir,
        String.format("my_buf_type_my_prefix#2param-a#2param-b#%d.buf", nanoSeconds2),
        "content2");
    createTempFile(
        backupDir,
        String.format("my_buf_type_my_prefix#3param_a#3param_b#%d.buf", nanoSeconds3),
        "content3");
    createTempFile(
        backupDir,
        String.format("my_buf_type_my_prefixz#param_a#param_b#%d.buf", System.nanoTime()),
        "ignored");
    createTempFile(
        backupDir,
        String.format("my_buf_type_my_prefi#param_a#param_b#%d.buf", System.nanoTime()),
        "ignored");
    createTempFile(
        backupDir,
        String.format("my_buf_type_my_prefix#param:a#param:b#%d.buf", System.nanoTime()),
        "ignored");
    createTempFile(
        backupDir,
        String.format("my_buf_type_my_prefix#param_a#param_b#%d", System.nanoTime()),
        "ignored");
    createTempFile(
        backupDir,
        String.format("my_buf_type_my_prefix#param_a#param_b#%d.buff", System.nanoTime()),
        "ignored");
    Buffer buffer = mock(Buffer.class);
    doReturn("my_buf_type").when(buffer).bufferFormatType();
    String prefix = "my_prefix";
    FileBackup fileBackup = new FileBackup(backupDir, buffer, prefix);

    List<FileBackup.SavedBuffer> savedFiles =
        fileBackup.getSavedFiles().stream()
            .sorted(Comparator.comparing(FileBackup.SavedBuffer::getPath))
            .collect(Collectors.toList());
    System.out.println(savedFiles);
    assertThat(savedFiles).size().isEqualTo(3);
    assertSavedBuffer(
        savedFiles.get(0),
        backupDir
            .toPath()
            .resolve(String.format("my_buf_type_my_prefix#1paramA#1paramB#%d.buf", nanoSeconds1)),
        "content1".getBytes(StandardCharsets.UTF_8),
        "1paramA",
        "1paramB");
    assertSavedBuffer(
        savedFiles.get(1),
        backupDir
            .toPath()
            .resolve(String.format("my_buf_type_my_prefix#2param-a#2param-b#%d.buf", nanoSeconds2)),
        "content2".getBytes(StandardCharsets.UTF_8),
        "2param-a",
        "2param-b");
    assertSavedBuffer(
        savedFiles.get(2),
        backupDir
            .toPath()
            .resolve(String.format("my_buf_type_my_prefix#3param_a#3param_b#%d.buf", nanoSeconds3)),
        "content3".getBytes(StandardCharsets.UTF_8),
        "3param_a",
        "3param_b");
  }

  @Test
  void saveBuffer() throws IOException {
    File backupDir = Files.createTempDirectory("test").toFile();
    backupDir.deleteOnExit();
    Buffer buffer = mock(Buffer.class);
    doReturn("my_buf_type").when(buffer).bufferFormatType();
    String prefix = "my_prefix";
    FileBackup fileBackup = new FileBackup(backupDir, buffer, prefix);
    long startNanos = System.nanoTime();
    fileBackup.saveBuffer(
        Arrays.asList("1paramA", "1paramB"),
        ByteBuffer.wrap("content1".getBytes(StandardCharsets.UTF_8)));
    fileBackup.saveBuffer(
        Arrays.asList("2param-a", "2param-b"),
        ByteBuffer.wrap("content2".getBytes(StandardCharsets.UTF_8)));
    fileBackup.saveBuffer(
        Arrays.asList("3param_a", "3param_b"),
        ByteBuffer.wrap("content3".getBytes(StandardCharsets.UTF_8)));
    long endNanos = System.nanoTime();

    List<File> savedFiles =
        Arrays.stream(Objects.requireNonNull(backupDir.listFiles()))
            .sorted(Comparator.comparing(File::toString))
            .collect(Collectors.toList());
    assertThat(savedFiles).size().isEqualTo(3);
    assertSavedFile(
        savedFiles.get(0),
        "my_buf_type",
        "my_prefix",
        startNanos,
        endNanos,
        "1paramA",
        "1paramB",
        "content1".getBytes(StandardCharsets.UTF_8));
    assertSavedFile(
        savedFiles.get(1),
        "my_buf_type",
        "my_prefix",
        startNanos,
        endNanos,
        "2param-a",
        "2param-b",
        "content2".getBytes(StandardCharsets.UTF_8));
    assertSavedFile(
        savedFiles.get(2),
        "my_buf_type",
        "my_prefix",
        startNanos,
        endNanos,
        "3param_a",
        "3param_b",
        "content3".getBytes(StandardCharsets.UTF_8));
  }
}
