package org.deephacks.jobpipe;

import java.util.Optional;

public class PipelineContext {
  public final TimeRange range;
  public final String targetTaskId;
  public final String[] args;
  public final Boolean verbose;

  public PipelineContext(TimeRange range, String taskId, Boolean verbose, String[] args) {
    this.range = range;
    this.targetTaskId = taskId;
    this.args = Optional.ofNullable(args).orElse(new String[0]);
    this.verbose = Optional.ofNullable(verbose).orElse(false);
  }

  public PipelineContext(TimeRange range) {
    this(range, null, null, null);
  }
}
