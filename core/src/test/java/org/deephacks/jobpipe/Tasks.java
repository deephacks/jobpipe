package org.deephacks.jobpipe;

public class Tasks {

  @TaskSpec(timeRange = TimeRangeType.DAY)
  public static class Task1 extends Task {
    FileOutput output;

    public Task1(TaskContext context) {
      super(context);
      this.output = new FileOutput();
    }

    @Override
    public void execute() {
      sleep(100);
      output.create();
    }

    @Override
    public TaskOutput getOutput() {
      return output;
    }
  }

  @TaskSpec(timeRange = TimeRangeType.MINUTE)
  public static class Task2 extends Task {
    FileOutput output;

    public Task2(TaskContext context) {
      super(context);
      this.output = new FileOutput();
    }

    @Override
    public void execute() {
      sleep(100);
      output.create();
    }

    @Override
    public TaskOutput getOutput() {
      return output;
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
