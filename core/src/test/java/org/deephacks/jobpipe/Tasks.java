package org.deephacks.jobpipe;

import java.io.File;

public class Tasks {

  @TaskSpec(timeRange = TimeRangeType.DAY)
  public static class Task1 extends Task {

    public Task1(TaskContext context) {
      super(context);
    }

    @Override
    public void execute() {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      getContext().createPath();
    }

    @Override
    public TaskOutput getOutput() {
      File file = getContext().getPath().toFile();
      return new TaskOutput() {
        @Override
        public boolean exist() {
          return file.exists();
        }

        @Override
        public Object get() {
          return file;
        }
      };
    }
  }

  @TaskSpec(timeRange = TimeRangeType.MINUTE)
  public static class Task2 extends Task {

    public Task2(TaskContext context) {
      super(context);
    }

    @Override
    public void execute() {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      getContext().createPath();
    }

    @Override
    public TaskOutput getOutput() {
      File file = getContext().getPath().toFile();
      return new TaskOutput() {
        @Override
        public boolean exist() {
          return file.exists();
        }

        @Override
        public Object get() {
          return file;
        }
      };
    }
  }

}
