package org.deephacks.jobpipe;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

/**
 * Annotated on {@link org.deephacks.jobpipe.Task} for specifying default
 * Task scheduling. This information may be overridden at runtime when building
 * a {@link org.deephacks.jobpipe.JobSchedule}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(value={TYPE})
public @interface TaskSpec {
  String id() default "";
  TimeRangeType timeRange() default TimeRangeType.HOUR;
}
