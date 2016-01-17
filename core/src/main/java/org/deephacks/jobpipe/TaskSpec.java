package org.deephacks.jobpipe;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(value={TYPE})
public @interface TaskSpec {
  String id() default "";
  TimeRangeType timeRange() default TimeRangeType.HOUR;
}
