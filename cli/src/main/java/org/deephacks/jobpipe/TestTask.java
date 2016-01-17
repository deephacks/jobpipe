package org.deephacks.jobpipe;

public class TestTask extends Task {

  public TestTask(TaskContext context) {
    super(context);
  }

  @Override
  public void execute() {
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
