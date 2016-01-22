package org.deephacks.jobpipe;

public interface JobObserver {
  void notify(TaskStatus status);
}
