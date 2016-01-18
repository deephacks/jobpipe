package org.deephacks.jobpipe;

import org.deephacks.jobpipe.Tasks.Task1;
import org.deephacks.jobpipe.Tasks.Task2;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.deephacks.jobpipe.TimeRangeType.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class JobSchedulerTest {

  /**
   * See dag.png in this directory
   */
  @Test
  public void testDirectedAsyclicGraph() {
    JobSchedule.newSchedule("2015-11-11T10:00")
      .task(Task1.class).id("1").timeRange(MINUTE).add()
      .task(Task1.class).id("4").timeRange(MINUTE).add()
      .task(Task1.class).id("10").timeRange(MINUTE).add()
      .task(Task1.class).id("12").timeRange(MINUTE).add()
      .task(Task1.class).id("11").timeRange(MINUTE).depIds("12").add()
      .task(Task1.class).id("9").timeRange(MINUTE).depIds("10", "11", "12").add()
      .task(Task1.class).id("6").timeRange(MINUTE).depIds("4", "9").add()
      .task(Task1.class).id("5").timeRange(MINUTE).depIds("4").add()
      .task(Task1.class).id("0").timeRange(MINUTE).depIds("1", "5", "6").add()
      .task(Task1.class).id("3").timeRange(MINUTE).depIds("5").add()
      .task(Task1.class).id("2").timeRange(MINUTE).depIds("0", "3").add()
      .task(Task1.class).id("7").timeRange(MINUTE).depIds("6").add()
      .task(Task1.class).id("8").timeRange(MINUTE).depIds("7").add()
      .execute().awaitFinish();
  }

  @Test
  public void testExecuteTaskId() {
    JobSchedule schedule = JobSchedule.newSchedule("2015-11-11T10:00")
      .task(Task1.class).id("1").timeRange(MINUTE).add()
      .task(Task1.class).id("4").timeRange(MINUTE).add()
      .task(Task1.class).id("10").timeRange(MINUTE).add()
      .task(Task1.class).id("12").timeRange(MINUTE).add()
      .task(Task1.class).id("11").timeRange(MINUTE).depIds("12").add()
      .task(Task1.class).id("9").timeRange(MINUTE).depIds("10", "11", "12").add()
      .task(Task1.class).id("6").timeRange(MINUTE).depIds("4", "9").add()
      .task(Task1.class).id("5").timeRange(MINUTE).depIds("4").add()
      .task(Task1.class).id("0").timeRange(MINUTE).depIds("1", "5", "6").add()
      .task(Task1.class).id("3").timeRange(MINUTE).depIds("5").add()
      .task(Task1.class).id("2").timeRange(MINUTE).depIds("0", "3").add()
      .task(Task1.class).id("7").timeRange(MINUTE).depIds("6").add()
      .task(Task1.class).id("8").timeRange(MINUTE).depIds("7").add()
      .targetTask("6")
      .execute();
    List<String> taskIds = schedule.getScheduledTasks().stream()
      .map(task -> task.getContext().getId()).collect(Collectors.toList());
    assertThat(taskIds.size(), is(6));
    assertTrue(taskIds.contains("4"));
    assertTrue(taskIds.contains("6"));
    assertTrue(taskIds.contains("9"));
    assertTrue(taskIds.contains("10"));
    assertTrue(taskIds.contains("11"));
    assertTrue(taskIds.contains("12"));
    schedule.awaitFinish();
  }

  @Test
  public void testExecutePipelineContext() {
    PipelineContext context = new PipelineContext(new TimeRange("2015-11-11T10:00"), "6", new String[]{"hello"});
    JobSchedule schedule = JobSchedule.newSchedule(context)
      .task(Task1.class).id("1").timeRange(MINUTE).add()
      .task(Task1.class).id("4").timeRange(MINUTE).add()
      .task(Task1.class).id("10").timeRange(MINUTE).add()
      .task(Task1.class).id("12").timeRange(MINUTE).add()
      .task(Task1.class).id("11").timeRange(MINUTE).depIds("12").add()
      .task(Task1.class).id("9").timeRange(MINUTE).depIds("10", "11", "12").add()
      .task(Task1.class).id("6").timeRange(MINUTE).depIds("4", "9").add()
      .task(Task1.class).id("5").timeRange(MINUTE).depIds("4").add()
      .task(Task1.class).id("0").timeRange(MINUTE).depIds("1", "5", "6").add()
      .task(Task1.class).id("3").timeRange(MINUTE).depIds("5").add()
      .task(Task1.class).id("2").timeRange(MINUTE).depIds("0", "3").add()
      .task(Task1.class).id("7").timeRange(MINUTE).depIds("6").add()
      .task(Task1.class).id("8").timeRange(MINUTE).depIds("7").add()
      .execute();
    List<String> taskIds = schedule.getScheduledTasks().stream()
      .map(task -> task.getContext().getId()).collect(Collectors.toList());
    assertThat(taskIds.size(), is(6));
    assertTrue(taskIds.contains("4"));
    assertTrue(taskIds.contains("6"));
    assertTrue(taskIds.contains("9"));
    assertTrue(taskIds.contains("10"));
    assertTrue(taskIds.contains("11"));
    assertTrue(taskIds.contains("12"));
    schedule.awaitFinish();
  }

  /**
   * Test that same task class can be scheduled with different time range types.
   */
  @Test
  public void testSameTaskDifferentTimeRange() throws InterruptedException {
    JobSchedule.newSchedule("2016-01-17T15:16")
      .task(Task1.class).id("1-sec").timeRange(SECOND).add()
      .task(Task1.class).id("1-min").timeRange(MINUTE).depIds("1-sec").add()
      .execute().awaitFinish();
  }

  @Test
  public void testDifferentTaskTypes() throws InterruptedException {
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    JobSchedule.newSchedule("2016-01-17T15:16")
      .task(Task1.class).timeRange(SECOND).add()
      .task(Task2.class).deps(Task1.class).executor(executor).add()
      .execute().awaitFinish();
  }
}
