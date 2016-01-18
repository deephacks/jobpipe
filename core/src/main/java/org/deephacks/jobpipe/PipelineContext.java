package org.deephacks.jobpipe;

public class PipelineContext {
  public final TimeRange range;
  public final String taskId;
  public final String[] args;

  public PipelineContext(TimeRange range, String taskId, String[] args) {
    this.range = range;
    this.taskId = taskId;
    this.args = args;
  }
}
