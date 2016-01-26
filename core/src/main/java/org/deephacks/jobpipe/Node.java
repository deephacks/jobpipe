package org.deephacks.jobpipe;

import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

class Node {
  private final String id;
  private final int scheduleId;
  private final TaskContext context;
  private final TimeRange range;
  private final List<Node> dependencies = new ArrayList<>();
  private final Task task;
  private final Scheduler scheduler;
  private final String[] args;
  private final AtomicReference<TaskStatus> status = new AtomicReference<>();

  Node(String id, int scheduleId, Task task, TimeRange range, Scheduler scheduler, String[] args, JobObserver observer, boolean verbose) {
    this.id = id;
    this.scheduleId = scheduleId;
    this.range = range;
    this.args = args;
    this.scheduler = scheduler;
    this.context = new TaskContext(this);
    this.task = task;
    this.status.set(new TaskStatus(context, observer, verbose));
  }

  void execute() {
    task.execute(context);
  }

  TaskContext getContext() {
    return context;
  }

  TaskOutput getTaskOutput() {
    return task.getOutput(context);
  }

  DateTime getTimeout() {
    return range.to();
  }

  Task getTask() {
    return task;
  }

  TaskStatus getStatus() {
    return status.get();
  }

  String[] getArgs() {
    return args;
  }

  String getId() {
    return id;
  }

  int getScheduleId() {
    return scheduleId;
  }

  TimeRange getRange() {
    return range;
  }

  boolean dependenciesDone() {
    for (Node n : dependencies) {
      if (!n.getStatus().isDone()) {
        return false;
      }
    }
    return true;
  }

  void addDependencies(Node... tasks) {
    dependencies.addAll(Arrays.asList(tasks));
  }

  /**
   * Get the direct neighbour dependencies of this node.
   */
  List<Node> getDirectDependencies() {
    return dependencies;
  }

  /**
   * Get all dependencies, direct and transitive, of this node.
   */
  Set<Node> getDependencies() {
    ArrayDeque<Node> deps = new ArrayDeque<>(dependencies);
    LinkedHashSet<Node> result = new LinkedHashSet<>();
    while (!deps.isEmpty()) {
      Node node = deps.poll();
      result.add(node);
      for (Node n : node.dependencies) {
        result.add(n);
        deps.add(n);
      }
    }
    return result;
  }

  boolean hasOutput() {
    return context.hasOutput();
  }

  Scheduler getScheduler() {
    return scheduler;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Node node = (Node) o;

    if (id != null ? !id.equals(node.id) : node.id != null) return false;
    return range != null ? range.equals(node.range) : node.range == null;

  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (range != null ? range.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "[" + id + "," + range.toString() + "]";
  }
}
