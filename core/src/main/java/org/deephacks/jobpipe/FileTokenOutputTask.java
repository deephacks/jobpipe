package org.deephacks.jobpipe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class FileTokenOutputTask implements Task {
  private String PATH_FORMAT = "/tmp/tasks/%s/%s";

  @Override
  public FileTokenOutput getOutput(TaskContext ctx) {
    String path = String.format(PATH_FORMAT, ctx.getId(), ctx.getTimeRange().format());
    return new FileTokenOutput(path);
  }

  public static final class FileTokenOutput implements TaskOutput {
    private final Path path;

    public FileTokenOutput(String path) {
      this.path = Paths.get(path);
    }

    public Path create() {
      try {
        return Files.createDirectories(path);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean exist() {
      return path.toFile().exists();
    }

    @Override
    public Object get() {
      return path.toFile();
    }
  }
}
