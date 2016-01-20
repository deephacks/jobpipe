package org.deephacks.jobpipe;

import org.joda.time.DateTime;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;

class Node {
  private final String id;
  private final TaskContext context;
  private final TimeRange range;
  private final List<Node> dependencies = new ArrayList<>();
  private final Task task;
  private final ScheduledExecutorService executor;
  private final String[] args;

  Node(String id, Class<? extends Task> cls, TimeRange range, ScheduledExecutorService executor, String[] args) {
    this.id = id;
    this.range = range;
    this.args = args;
    this.executor = executor;
    this.context = new TaskContext(this);
    this.task = newTask(cls, context);
  }

  void execute() {
    task.execute();
  }

  TaskOutput getTaskOutput() {
    return task.getOutput();
  }

  DateTime getTimeout() {
    return range.from();
  }

  Task getTask() {
    return task;
  }

  public String[] getArgs() {
    return args;
  }

  String getId() {
    return id;
  }

  TimeRange getRange() {
    return range;
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

  boolean isFinished() {
    return context.isFinished();
  }

  Task newTask(Class<? extends Task> cls, TaskContext context) {
    try {
      Constructor<? extends Task> constructor = cls.getConstructor(TaskContext.class);
      return constructor.newInstance(context);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  ScheduledExecutorService getExecutor() {
    return executor;
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
