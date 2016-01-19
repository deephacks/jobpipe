package org.deephacks.jobpipe;

import java.util.Arrays;

public class TestTask extends FileTokenOutputTask {

  public TestTask(TaskContext context) {
    super(context);
  }

  @Override
  public void execute() {
    System.out.println(Arrays.asList(getContext().getArgs()));
    System.out.println(output.create().toFile().getAbsolutePath());
  }
}
