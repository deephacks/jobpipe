package org.deephacks.jobpipe;

public interface Pipeline {
  void execute(PipelineContext context);
}
