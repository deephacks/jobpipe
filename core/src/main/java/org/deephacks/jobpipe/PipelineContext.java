package org.deephacks.jobpipe;

public class PipelineContext {
  public final TimeRange range;
  public final String taskId;

  public PipelineContext(TimeRange range, String taskId) {
    this.range = range;
    this.taskId = taskId;
  }
}
