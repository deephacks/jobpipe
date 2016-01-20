package org.deephacks.jobpipe;

public abstract class Task {
  private TaskContext context;

  public Task(TaskContext context) {
    this.context = context;
  }

  public abstract void execute();

  public abstract TaskOutput getOutput();

  public TaskContext getContext() {
    return context;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Task task = (Task) o;

    return context != null ? context.equals(task.context) : task.context == null;
  }

  @Override
  public int hashCode() {
    return context != null ? context.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "[" + context.id + "," + context.node.getRange() + "]";
  }
}
