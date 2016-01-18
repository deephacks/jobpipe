package org.deephacks.jobpipe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TaskContext {
  final String id;
  final Node node;
  final Path path;
  final String[] args;

  TaskContext(Node node) {
    this.id = node.getId();
    this.node = node;
    this.path = Paths.get(Config.BASE_PATH + "/tasks/" + id + "/" + node.getRange().format());
    this.args = node.getArgs();
  }

  public boolean isFinished() {
    return path.toFile().exists();
  }

  public String getId() {
    return id;
  }

  public Path getPath() {
    return path;
  }

  public String[] getArgs() {
    return args;
  }

  public Path createPath() {
    try {
      Files.createDirectories(path);
      return path;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public List<TaskOutput> getDependecyOutput() {
    return node.getDependencies().stream()
      .map(n -> n.getTaskOutput())
      .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return path.toString();
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
