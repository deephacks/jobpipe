package org.deephacks.jobpipe;

public class TestPipeline implements Pipeline {

  @Override
  public void execute(PipelineContext context) {
    JobSchedule.newSchedule(context.range)
      .task(TestTask.class).id("1").timeRange(TimeRangeType.SECOND).addTask()
      .task(TestTask.class).id("2").timeRange(TimeRangeType.SECOND).depIds("1").addTask()
      .task(TestTask.class).id("3").timeRange(TimeRangeType.SECOND).depIds("2").addTask()
      .execute(context.taskId)
      .awaitFinish();
  }
}
