package org.deephacks.jobpipe;

import java.util.concurrent.TimeUnit;

public interface Scheduler {
  void schedule(final Runnable runnable, final long delayTime, final TimeUnit unit);

  void shutdown();
}
