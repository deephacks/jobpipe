package org.deephacks.jobpipe;

/**
 * A task should preferably be idempotent, so that running it many times
 * gives the same outcome as running it once.
 *
 * A task may be annotated with {@link org.deephacks.jobpipe.TaskSpec} default values.
 */
public abstract class Task {
  private TaskContext context;

  public Task(TaskContext context) {
    this.context = context;
  }

  /**
   * Executes when all dependent tasks have executed.
   *
   * Execution will be skipped if the task has previously valid output.
   *
   * Any exception thrown will fail this task and tasks that depends on it.
   */
  public abstract void execute();

  /**
   * @return a persistent location of output, like a local directory or HDFS path.
   */
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
