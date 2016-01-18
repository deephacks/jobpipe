package org.deephacks.jobpipe;

import java.util.Arrays;

public class TestTask extends Task {

  public TestTask(TaskContext context) {
    super(context);
  }

  @Override
  public void execute() {
    System.out.println(Arrays.asList(getContext().getArgs()));
    System.out.println(getContext().createPath());
  }

  @Override
  public TaskOutput getOutput() {
    return new TaskOutput() {
      @Override
      public boolean exist() {
        return getContext().getPath().toFile().exists();
      }

      @Override
      public Object get() {
        return getContext().getPath().toFile();
      }
    };
  }
}
