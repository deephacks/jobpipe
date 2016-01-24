package org.deephacks.jobpipe;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DefaultScheduler implements Scheduler {
  ScheduledThreadPoolExecutor executor;

  public DefaultScheduler() {
    this.executor = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors());
  }

  public DefaultScheduler(int corePoolSize) {
    this.executor = new ScheduledThreadPoolExecutor(corePoolSize);
  }

  public DefaultScheduler(ScheduledThreadPoolExecutor executor) {
    this.executor = executor;
  }

  @Override
  public void schedule(Runnable runnable, long delayTime, TimeUnit unit) {
    executor.schedule(runnable, delayTime, unit);
  }

  @Override
  public void shutdown() {
    this.executor.shutdownNow();
  }
}
