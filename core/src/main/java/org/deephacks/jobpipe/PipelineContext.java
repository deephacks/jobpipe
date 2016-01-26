package org.deephacks.jobpipe;

import java.util.Optional;

public class PipelineContext {
  public final TimeRange range;
  public final String targetTaskId;
  public final String[] args;
  public final Boolean verbose;
  JobSchedule schedule;

  public PipelineContext(TimeRange range, String taskId, Boolean verbose, String[] args) {
    this.range = range;
    this.targetTaskId = taskId;
    this.args = Optional.ofNullable(args).orElse(new String[0]);
    this.verbose = Optional.ofNullable(verbose).orElse(false);
  }

  public PipelineContext(TimeRange range) {
    this(range, null, null, null);
  }

  /**
   * Called when the job schedule get created, this is a ugly hack
   * so that the CLI can check the status after it has finished.
   */
  void setSchedule(JobSchedule schedule) {
    this.schedule = schedule;
  }
}
