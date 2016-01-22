package org.deephacks.jobpipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobObserverLog implements JobObserver {
  private static final Logger logger = LoggerFactory.getLogger(JobObserverLog.class);

  @Override
  public boolean notify(TaskStatus status) {
    logger.info("{} -> {}", status.getContext(), status.code());
    return true;
  }
}
