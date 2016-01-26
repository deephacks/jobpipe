package org.deephacks.jobpipe;

/**
 * Can be used for logging, storage history of task transitions,
 * rejection tasks and similar.
 */
public interface JobObserver {
  /**
   * Called before a task transition followed by the execution of the new status.
   * Some TaskStatusCode transitions can be rejected, in particular
   * {@link org.deephacks.jobpipe.TaskStatus.TaskStatusCode#SCHEDULED},
   * which would reject scheduling of the task.
   *
   * Do not throw runtime exceptions, it will reject the task transition and barf
   * the stacktrace on System.err.
   *
   * @param status next status.
   * @return false to reject the status change and set status to
   * {@link org.deephacks.jobpipe.TaskStatus.TaskStatusCode#ERROR_ABORTED}
   */
  boolean notify(TaskStatus status);
}
