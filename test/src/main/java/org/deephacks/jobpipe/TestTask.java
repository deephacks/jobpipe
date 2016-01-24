package org.deephacks.jobpipe;

import java.util.Arrays;

public class TestTask extends FileTokenOutputTask {

  @Override
  public void execute(TaskContext ctx) {
    System.out.println(Arrays.asList(ctx.getArgs()));
    System.out.println(getOutput(ctx).create().toFile().getAbsolutePath());
  }
}
