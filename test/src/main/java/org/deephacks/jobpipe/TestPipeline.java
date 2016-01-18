package org.deephacks.jobpipe;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class TestPipeline implements Pipeline {

  @Override
  public void execute(PipelineContext context) {
    OptionParser parser = new OptionParser();
    parser.allowsUnrecognizedOptions();
    OptionSpec<String> optTest = parser.accepts("param1", "Test args parsing")
      .withRequiredArg().ofType(String.class).describedAs("param1");
    OptionSet options = parser.parse(context.args);
    if (options.has("param1")) {
      System.out.println("Found option: " + options.valueOf(optTest));
    }

    JobSchedule.newSchedule(context.range)
      .task(TestTask.class).id("1").timeRange(TimeRangeType.SECOND).addTask()
      .task(TestTask.class).id("2").timeRange(TimeRangeType.SECOND).depIds("1").addTask()
      .task(TestTask.class).id("3").timeRange(TimeRangeType.SECOND).depIds("2").addTask()
      .execute(context.taskId)
      .awaitFinish();
  }
}
