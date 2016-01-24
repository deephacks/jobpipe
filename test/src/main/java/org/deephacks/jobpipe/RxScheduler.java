package org.deephacks.jobpipe;

import rx.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;

public class RxScheduler implements Scheduler {

  @Override
  public void schedule(Runnable runnable, long delayTime, TimeUnit unit) {
    Schedulers.computation().createWorker()
      .schedule(() -> runnable.run(), delayTime, unit);
  }
}
