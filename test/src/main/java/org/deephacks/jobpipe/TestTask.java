package org.deephacks.jobpipe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class TestTask extends Task {
  Path path;

  public TestTask(TaskContext context) {
    super(context);
    this.path = Paths.get("/tmp/tasks/" + getContext().getId() + "/" + getContext().getTimeRange().format());
  }

  @Override
  public void execute() {
    System.out.println(Arrays.asList(getContext().getArgs()));

    try {
      Files.createDirectories(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    System.out.println(path.toFile().getAbsolutePath());
  }

  @Override
  public TaskOutput getOutput() {
    return new TaskOutput() {
      @Override
      public boolean exist() {
        return path.toFile().exists();
      }

      @Override
      public Object get() {
        return path.toFile();
      }
    };
  }
}
