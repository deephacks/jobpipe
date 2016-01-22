package org.deephacks.jobpipe;

import java.util.Optional;

public class TaskStatus {
  private TaskContext context;
  private Object failReason;
  private TaskStatusCode code;
  private long lastUpdate = 0;
  private final JobObserver observer;

  public TaskStatus(TaskContext context, JobObserver observer) {
    this.context = context;
    this.observer = observer;
    setCode(TaskStatusCode.NEW);
    setLastUpdate();
  }

  public Optional<Object> getFailReason() {
    return Optional.ofNullable(failReason);
  }

  public TaskStatusCode code() {
    return code;
  }

  public boolean hasFailed() {
    return TaskStatusCode.ERROR_DEPENDENCY == code ||
      TaskStatusCode.ERROR_EXECUTE == code;
  }

  public TaskContext getContext() {
    return context;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  boolean setCode(TaskStatusCode code) {
    if (this.code != code) {
      this.code = code;
    }
    setLastUpdate();
    try {
      return observer != null ? observer.notify(this) : true;
    } catch (Throwable e) {
      e.printStackTrace(System.err);
      return false;
    }
  }

  void failed(Throwable e) {
    this.failReason = e;
    setCode(TaskStatusCode.ERROR_EXECUTE);
  }

  void failedDep(TaskContext failedDep) {
    this.failReason = failedDep;
    setCode(TaskStatusCode.ERROR_DEPENDENCY);
  }

  void finished() {
    setCode(TaskStatusCode.FINISHED);
  }

  void skipped() {
    setCode(TaskStatusCode.SKIPPED);
  }

  boolean running() {
    return setCode(TaskStatusCode.RUNNING);
  }

  boolean scheduled() {
    return setCode(TaskStatusCode.SCHEDULED);
  }

  void setLastUpdate() {
    this.lastUpdate = System.currentTimeMillis();
  }

  public enum TaskStatusCode {
    NEW, SCHEDULED, FINISHED, SKIPPED, RUNNING, ERROR_EXECUTE, ERROR_DEPENDENCY, ABORTED
  }
}
