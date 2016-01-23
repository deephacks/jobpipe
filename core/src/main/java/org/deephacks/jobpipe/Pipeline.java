package org.deephacks.jobpipe;

/**
 * A pipeline schedule generated at runtime.
 */
public interface Pipeline {

  /**
   * Called when the pipeline should execute, usually triggered from command line.
   *
   * @param context information of how the pipeline should be executed.
   */
  void execute(PipelineContext context);
}
