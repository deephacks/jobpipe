package org.deephacks.jobpipe;

public interface TaskOutput {
  /**
   * @return if the output exist.
   */
  boolean exist();

  /**
   * @return the actual output, like a File or similar.
   */
  Object get();
}