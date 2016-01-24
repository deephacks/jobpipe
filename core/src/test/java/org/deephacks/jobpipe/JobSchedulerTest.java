package org.deephacks.jobpipe;

import org.deephacks.jobpipe.TaskStatus.TaskStatusCode;
import org.deephacks.jobpipe.Tasks.Task1;
import org.deephacks.jobpipe.Tasks.Task2;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.deephacks.jobpipe.Tasks.sleep;
import static org.deephacks.jobpipe.TimeRangeType.MINUTE;
import static org.deephacks.jobpipe.TimeRangeType.SECOND;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class JobSchedulerTest {
  private JobObserver observer = new JobObserverLog();
  /**
   * See dag.png in this directory
   */
  @Test
  public void testDirectedAsyclicGraph() {
    Task1 task = new Task1();
    JobSchedule schedule = JobSchedule.newSchedule("2015-01-14T10:00")
      .observer(observer)
      .task(task).id("1").timeRange(MINUTE).add()
      .task(task).id("4").timeRange(MINUTE).add()
      .task(task).id("10").timeRange(MINUTE).add()
      .task(task).id("12").timeRange(MINUTE).add()
      .task(task).id("11").timeRange(MINUTE).depIds("12").add()
      .task(task).id("9").timeRange(MINUTE).depIds("10", "11", "12").add()
      .task(task).id("6").timeRange(MINUTE).depIds("4", "9").add()
      .task(task).id("5").timeRange(MINUTE).depIds("4").add()
      .task(task).id("0").timeRange(MINUTE).depIds("1", "5", "6").add()
      .task(task).id("3").timeRange(MINUTE).depIds("5").add()
      .task(task).id("2").timeRange(MINUTE).depIds("0", "3").add()
      .task(task).id("7").timeRange(MINUTE).depIds("6").add()
      .task(task).id("8").timeRange(MINUTE).depIds("7").add()
      .execute();
    List<String> taskIds = schedule.getScheduledTasks().stream()
      .map(t -> t.getContext().getId()).collect(Collectors.toList());
    assertThat(taskIds.size(), is(13));
    assertTrue(taskIds.containsAll(Arrays.asList(
      "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")));
    schedule.awaitFinish();
  }

  @Test
  public void testExecuteTaskId() {
    Task1 task = new Task1();
    JobSchedule schedule = JobSchedule.newSchedule("2015-12-01T10:00")
      .observer(observer)
      .task(task).id("1").timeRange(MINUTE).add()
      .task(task).id("4").timeRange(MINUTE).add()
      .task(task).id("10").timeRange(MINUTE).add()
      .task(task).id("12").timeRange(MINUTE).add()
      .task(task).id("11").timeRange(MINUTE).depIds("12").add()
      .task(task).id("9").timeRange(MINUTE).depIds("10", "11", "12").add()
      .task(task).id("6").timeRange(MINUTE).depIds("4", "9").add()
      .task(task).id("5").timeRange(MINUTE).depIds("4").add()
      .task(task).id("0").timeRange(MINUTE).depIds("1", "5", "6").add()
      .task(task).id("3").timeRange(MINUTE).depIds("5").add()
      .task(task).id("2").timeRange(MINUTE).depIds("0", "3").add()
      .task(task).id("7").timeRange(MINUTE).depIds("6").add()
      .task(task).id("8").timeRange(MINUTE).depIds("7").add()
      .targetTask("6")
      .execute();
    List<String> taskIds = schedule.getScheduledTasks().stream()
      .map(t -> t.getContext().getId()).collect(Collectors.toList());
    assertThat(taskIds.size(), is(6));
    assertTrue(taskIds.containsAll(Arrays.asList(
      "4", "6", "9", "10", "11", "12")));
    schedule.awaitFinish();
  }

  @Test
  public void testExecutePipelineContext() {
    Task1 task = new Task1();
    TimeRange range = new TimeRange("2012-10-10T10:00");
    String taskId = "0";
    String[] args = new String[]{"hello"};
    PipelineContext context = new PipelineContext(range, taskId, args);
    JobSchedule schedule = JobSchedule.newSchedule(context)
      .observer(observer)
      .task(task).id("1").timeRange(MINUTE).add()
      .task(task).id("4").timeRange(MINUTE).add()
      .task(task).id("10").timeRange(MINUTE).add()
      .task(task).id("12").timeRange(MINUTE).add()
      .task(task).id("11").timeRange(MINUTE).depIds("12").add()
      .task(task).id("9").timeRange(MINUTE).depIds("10", "11", "12").add()
      .task(task).id("6").timeRange(MINUTE).depIds("4", "9").add()
      .task(task).id("5").timeRange(MINUTE).depIds("4").add()
      .task(task).id("0").timeRange(MINUTE).depIds("1", "5", "6").add()
      .task(task).id("3").timeRange(MINUTE).depIds("5").add()
      .task(task).id("2").timeRange(MINUTE).depIds("0", "3").add()
      .task(task).id("7").timeRange(MINUTE).depIds("6").add()
      .task(task).id("8").timeRange(MINUTE).depIds("7").add()
      .execute();
    List<String> taskIds = schedule.getScheduledTasks().stream()
      .map(t -> t.getContext().getId()).collect(Collectors.toList());
    assertThat(taskIds.size(), is(9));
    assertTrue(taskIds.containsAll(Arrays.asList(
      "4", "6", "9", "10", "11", "12", "0", "1")));
    schedule.awaitFinish();
  }

  /**
   * Test that same task class can be scheduled with different time range types.
   */
  @Test
  public void testSameTaskDifferentTimeRange() {
    Task1 task = new Task1();
    JobSchedule schedule = JobSchedule.newSchedule("2011-10-17T15:16")
      .task(task).id("1-sec").timeRange(SECOND).add()
      .task(task).id("1-min").timeRange(MINUTE).depIds("1-sec").add()
      .execute();
    List<TaskStatus> tasks = schedule.getScheduledTasks();
    assertThat(tasks.size(), is(60 + 1));
    schedule.awaitFinish();
  }

  @Test
  public void testTooShortTimePeriod() {
    JobSchedule schedule = JobSchedule.newSchedule("2006-01-17T15:16:01")
      .task(new Task1()).id("1-min").timeRange(MINUTE).add()
      .execute();
    assertThat(schedule.getScheduledTasks().size(), is(0));
  }

  @Test
  public void testMissingDep() {
    try {
      JobSchedule.newSchedule("2000-01-17T15:16")
        .task(new Task1()).id("1-min").timeRange(MINUTE).depIds("missing").add()
        .execute();
      fail("should fail");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("missing"));
    }
  }

  @Test
  public void testDifferentTaskTypes() {
    Scheduler scheduler = new DefaultScheduler(1);
    JobSchedule.newSchedule("2011-01-17T15:16")
      .observer(observer)
      .task(new Task1()).timeRange(SECOND).add()
      .task(new Task2()).deps(Task1.class).scheduler(scheduler).add()
      .execute().awaitFinish();
  }

  @Test
  public void testOutputFromDependency() {
    JobSchedule.newSchedule("2013-01-17T15:16")
      .task(new Task1()).timeRange(SECOND).add()
      .task(new CheckOutputTask()).timeRange(TimeRangeType.MINUTE).deps(Task1.class).add()
      .execute().awaitFinish();
  }


  @Test(timeout = 15_000)
  public void testFailedTaskAbortsExecution() {
    for (int i = 0; i < 3; i++) {
      JobSchedule schedule = JobSchedule.newSchedule("1999-01-17")
        .observer(observer)
        .task(new FailingTask()).timeRange(TimeRangeType.HOUR).add()
        .task(new Task1()).timeRange(TimeRangeType.DAY).deps(FailingTask.class).add()
        .execute();
      schedule.awaitFinish();
      List<TaskStatus> tasks = schedule.getScheduledTasks();
      assertThat(tasks.size(), is(24 + 1));

      List<TaskStatus> errors = tasks.stream()
        .filter(t -> t.getContext().getStatus().code() == TaskStatusCode.ERROR_EXECUTE)
        .collect(Collectors.toList());
      assertThat(errors.size(), is(24));
      for (TaskStatus t : errors) {
        assertThat(t.getContext().getId(), is("FailingTask"));
      }

      List<TaskStatus> deps = tasks.stream()
        .filter(t -> t.getContext().getStatus().code() == TaskStatusCode.ERROR_DEPENDENCY)
        .collect(Collectors.toList());
      assertThat(deps.size(), is(1));
      assertThat(deps.get(0).getContext().getId(), is("Task1"));
    }
  }

  public static class FailingTask implements Task {

    @Override
    public void execute(TaskContext ctx) {
      throw new RuntimeException("message");
    }

    @Override
    public TaskOutput getOutput(TaskContext ctx) {
      return new TmpFileOutput();
    }
  }

  public static class CheckOutputTask implements Task {
    TmpFileOutput output = new TmpFileOutput();

    @Override
    public void execute(TaskContext ctx) {
      List<File> files = ctx.getDependecyOutput().stream()
        .map(o -> (File) o.get()).peek(file -> assertTrue(file.exists()))
        .collect(Collectors.toList());
      assertThat(files.size(), is(60));
      output.create();
    }

    @Override
    public TaskOutput getOutput(TaskContext ctx) {
      return output;
    }
  }
}
