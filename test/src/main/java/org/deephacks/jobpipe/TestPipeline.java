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

    JobSchedule.newSchedule(context)
      .scheduler(new RxScheduler())
      .observer(new JobObserverLog())
      .task(new TestTask()).id("1").timeRange(TimeRangeType.HOUR).add()
      .task(new TestTask()).id("2").timeRange(TimeRangeType.HOUR).depIds("1").add()
      .task(new TestTask()).id("3").timeRange(TimeRangeType.HOUR).depIds("2").add()
      .execute()
      .awaitFinish();
  }
}
