package org.deephacks.jobpipe;

public class Tasks {

  @TaskSpec(timeRange = TimeRangeType.DAY)
  public static class Task1 extends Task {

    public Task1(TaskContext context) {
      super(context);
    }

    @Override
    public void execute() {
      sleep(100);
      getContext().createPath();
    }

    @Override
    public TaskOutput getOutput() {
      return new FileOutput(getContext().getPath().toFile());
    }
  }

  @TaskSpec(timeRange = TimeRangeType.MINUTE)
  public static class Task2 extends Task {

    public Task2(TaskContext context) {
      super(context);
    }

    @Override
    public void execute() {
      sleep(100);
      getContext().createPath();
    }

    @Override
    public TaskOutput getOutput() {
      return new FileOutput(getContext().getPath().toFile());
    }
  }

  public static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
