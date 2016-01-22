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

  /**
   * See dag.png in this directory
   */
  @Test
  public void testDirectedAsyclicGraph() {
    JobSchedule schedule = JobSchedule.newSchedule("2015-01-14T10:00")
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
    assertThat(taskIds.size(), is(13));
    assertTrue(taskIds.containsAll(Arrays.asList(
      "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")));
    schedule.awaitFinish();
  }

  @Test
  public void testExecuteTaskId() {
    JobSchedule schedule = JobSchedule.newSchedule("2015-12-01T10:00")
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
    assertTrue(taskIds.containsAll(Arrays.asList(
      "4", "6", "9", "10", "11", "12")));
    schedule.awaitFinish();
  }

  @Test
  public void testExecutePipelineContext() {
    TimeRange range = new TimeRange("2012-10-10T10:00");
    String taskId = "0";
    String[] args = new String[]{"hello"};
    PipelineContext context = new PipelineContext(range, taskId, args);
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
    JobSchedule schedule = JobSchedule.newSchedule("2011-10-17T15:16")
      .task(Task1.class).id("1-sec").timeRange(SECOND).add()
      .task(Task1.class).id("1-min").timeRange(MINUTE).depIds("1-sec").add()
      .execute();
    List<Task> tasks = schedule.getScheduledTasks();
    assertThat(tasks.size(), is(60 + 1));
    schedule.awaitFinish();
  }

  @Test
  public void testTooShortTimePeriod() {
    JobSchedule schedule = JobSchedule.newSchedule("2006-01-17T15:16:01")
      .task(Task1.class).id("1-min").timeRange(MINUTE).add()
      .execute();
    assertThat(schedule.getScheduledTasks().size(), is(0));
  }

  @Test
  public void testMissingDep() {
    try {
      JobSchedule.newSchedule("2000-01-17T15:16")
        .task(Task1.class).id("1-min").timeRange(MINUTE).depIds("missing").add()
        .execute();
      fail("should fail");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("missing"));
    }
  }

  @Test
  public void testDifferentTaskTypes() {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    JobSchedule.newSchedule("2011-01-17T15:16")
      .task(Task1.class).timeRange(SECOND).add()
      .task(Task2.class).deps(Task1.class).executor(executor).add()
      .execute().awaitFinish();
  }

  @Test
  public void testOutputFromDependency() {
    JobSchedule.newSchedule("2013-01-17T15:16")
      .task(Task1.class).timeRange(SECOND).add()
      .task(CheckOutputTask.class).timeRange(TimeRangeType.MINUTE).deps(Task1.class).add()
      .execute().awaitFinish();
  }


  @Test(timeout = 15_000)
  public void testFailedTaskAbortsExecution() {
    for (int i = 0; i < 3; i++) {
      JobSchedule schedule = JobSchedule.newSchedule("1999-01-17")
        .task(FailingTask.class).timeRange(TimeRangeType.HOUR).add()
        .task(Task1.class).timeRange(TimeRangeType.DAY).deps(FailingTask.class).add()
        .execute();
      schedule.awaitFinish();
      List<Task> tasks = schedule.getScheduledTasks();
      assertThat(tasks.size(), is(24 + 1));

      List<Task> errors = tasks.stream()
        .filter(t -> t.getContext().getStatus().code() == TaskStatusCode.ERROR_EXECUTE)
        .collect(Collectors.toList());
      assertThat(errors.size(), is(24));
      for (Task t : errors) {
        assertThat(t.getContext().getId(), is("FailingTask"));
      }

      List<Task> deps = tasks.stream()
        .filter(t -> t.getContext().getStatus().code() == TaskStatusCode.ERROR_DEPENDENCY)
        .collect(Collectors.toList());
      assertThat(deps.size(), is(1));
      assertThat(deps.get(0).getContext().getId(), is("Task1"));
    }
  }

  public static class FailingTask extends Task {
    FileOutput output;

    public FailingTask(TaskContext context) {
      super(context);
      this.output = new FileOutput();
    }

    @Override
    public void execute() {
      sleep(500);
      throw new RuntimeException("message");
    }

    @Override
    public TaskOutput getOutput() {
      return output;
    }
  }

  public static class CheckOutputTask extends Task {
    FileOutput output;

    public CheckOutputTask(TaskContext context) {
      super(context);
      this.output = new FileOutput();
    }

    @Override
    public void execute() {
      List<File> files = getContext().getDependecyOutput().stream()
        .map(o -> (File) o.get()).peek(file -> assertTrue(file.exists()))
        .collect(Collectors.toList());
      assertThat(files.size(), is(60));
      output.create();
    }

    @Override
    public TaskOutput getOutput() {
      return output;
    }
  }
}
