package org.deephacks.jobpipe;

public class Tasks {

  @TaskSpec(timeRange = TimeRangeType.DAY)
  public static class Task1 implements Task {
    TmpFileOutput output = new TmpFileOutput();

    @Override
    public void execute(TaskContext ctx) {
      output.create();
    }

    @Override
    public TaskOutput getOutput(TaskContext ctx) {
      return output;
    }
  }

  @TaskSpec(timeRange = TimeRangeType.MINUTE)
  public static class Task2 implements Task {
    TmpFileOutput output = new TmpFileOutput();

    @Override
    public void execute(TaskContext ctx) {
      output.create();
    }

    @Override
    public TaskOutput getOutput(TaskContext ctx) {
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
