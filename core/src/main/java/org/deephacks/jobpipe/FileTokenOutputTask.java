package org.deephacks.jobpipe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class FileTokenOutputTask extends Task {
  protected FileTokenOutput output;

  public FileTokenOutputTask(TaskContext context) {
    super(context);
    Path path = Paths.get("/tmp/tasks/" + getContext().getId() + "/" + getContext().getTimeRange().format());
    this.output = new FileTokenOutput(path);
  }

  @Override
  public TaskOutput getOutput() {
    return output;
  }

  public static final class FileTokenOutput implements TaskOutput {
    private final Path path;

    public FileTokenOutput(Path path) {
      this.path = path;
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
