package org.deephacks.jobpipe;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Provides information regarding the current task execution.
 */
public class TaskContext {
  final String id;
  final Node node;
  final String[] args;

  TaskContext(Node node) {
    this.id = node.getId();
    this.node = node;
    this.args = node.getArgs() == null ? new String[0] : node.getArgs();
  }

  public boolean hasOutput() {
    return node.getTask().getOutput(this).exist();
  }

  /**
   * @return the time range that this task
   */
  public TimeRange getTimeRange() {
    return node.getRange();
  }

  /**
   * @return task id
   */
  public String getId() {
    return id;
  }

  /**
   * @return current status of this task
   */
  public TaskStatus getStatus() {
    return node.getStatus();
  }

  /**
   * @return arguments given when the pipeline was started.
   */
  public String[] getArgs() {
    return args;
  }

  /**
   * @return output from this task dependent tasks.
   */
  public List<TaskOutput> getDependecyOutput() {
    return node.getDependencies().stream()
      .map(n -> n.getTaskOutput())
      .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return node.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TaskContext context = (TaskContext) o;

    if (id != null ? !id.equals(context.id) : context.id != null) return false;
    return node != null ? node.equals(context.node) : context.node == null;

  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (node != null ? node.hashCode() : 0);
    return result;
  }
}
