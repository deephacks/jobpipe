package org.deephacks.jobpipe;

import java.util.Optional;

public class TaskStatus {
  private TaskContext context;
  private Object failReason;
  private TaskStatusCode code;
  private long lastUpdate = 0;
  private final JobObserver observer;
  private final boolean verbose;

  public TaskStatus(TaskContext context, JobObserver observer, boolean verbose) {
    this.context = context;
    this.observer = observer;
    this.verbose = verbose;
    setCode(TaskStatusCode.NEW);
    setLastUpdate();
  }

  public Optional<Object> getFailReason() {
    return Optional.ofNullable(failReason);
  }

  public TaskStatusCode code() {
    return code;
  }

  public boolean isDone() {
    return code != TaskStatusCode.NEW
      && code != TaskStatusCode.RUNNING
      && code != TaskStatusCode.SCHEDULED;
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
      if (verbose) {
        System.out.println(context + " -> " + this.code);
        if (code == TaskStatusCode.ERROR_EXECUTE) {
          ((Throwable) this.failReason).printStackTrace(System.err);
        }
      }
    }
    setLastUpdate();
    try {
      return observer != null ? observer.notify(this) : true;
    } catch (Throwable e) {
      if (verbose) {
        e.printStackTrace(System.err);
      }
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
