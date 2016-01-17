package org.deephacks.jobpipe;

public interface TaskOutput {
  boolean exist();
  Object get();
}