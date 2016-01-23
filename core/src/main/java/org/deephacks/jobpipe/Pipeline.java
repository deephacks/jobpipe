package org.deephacks.jobpipe;

/**
 * A pipeline schedule generated at runtime. A pipeline must provide a {@link java.util.ServiceLoader}
 * META-INF/services/org.deephacks.jobpipe.Pipeline file in its jar.
 */
public interface Pipeline {

  /**
   * Called when the pipeline should execute, usually triggered from command line.
   *
   * @param context information of how the pipeline should be executed.
   */
  void execute(PipelineContext context);
}
