package org.deephacks.jobpipe;

import java.util.List;
import java.util.stream.Collectors;

public class TaskContext {
  final String id;
  final Node node;
  final String[] args;

  TaskContext(Node node) {
    this.id = node.getId();
    this.node = node;
    this.args = node.getArgs();
  }

  public boolean isFinished() {
    return node.getTask().getOutput().exist();
  }

  public TimeRange getTimeRange() {
    return node.getRange();
  }

  public String getId() {
    return id;
  }

  public String[] getArgs() {
    return args;
  }

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
