package org.deephacks.jobpipe;

public class Config {
  public static final String BASE_PATH = System.getProperty("job.basePath", "/tmp");
  public static final long FAILED_TASK_RETRY_SEC = Long.getLong("job.task_retry_ms_failures", 1);
}
