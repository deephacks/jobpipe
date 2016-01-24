package org.deephacks.jobpipe;

/**
 * A task should preferably be idempotent, so that running it many times
 * gives the same outcome as running it once.
 *
 * A task may be annotated with {@link org.deephacks.jobpipe.TaskSpec} default values.
 */
public interface Task {

  /**
   * Executes when all dependent tasks have executed.
   *
   * Execution will be skipped if the task has previously valid output.
   *
   * Any exception thrown will fail this task and tasks that depends on it.
   *
   * @param context Provides information regarding the execution.
   */
  void execute(TaskContext context);

  /**
   * @return a persistent location of output, like a local directory or HDFS path.
   */
  <T extends TaskOutput> T getOutput(TaskContext context);

}
