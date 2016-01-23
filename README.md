# jobpipe
Java scheduler for pipelines of long-running batch processes, inspired by Spotify [Luigi](https://github.com/spotify/luigi).

The purpose of jobpipe is to execute certain tasks at regular time ranges and allow expressing dependencies
between tasks as a sequence of continuous executions in time. These tasks can be anything, like Hadoop/Spark jobs, log data processing or time series downsampling. A schedule is a time dependent directed acyclic graph of task executions. Every task produce output which is provided as input to dependent tasks. Any task is stalled until its dependent task output are produced (if any). Task output enable resumability of pipelines that crash halfway. A task that fail (without a previously valid output) will transitively fail its dependent tasks. Tasks may execute in parallel if their inputs are satisfied.

The execution model of jobpipe is similar to that of a compiler, with the added dimension of time ranges. 

### Example

Abstract [Pipeline](https://github.com/deephacks/jobpipe/blob/master/core/src/main/java/org/deephacks/jobpipe/Pipeline.java) of [Task](https://github.com/deephacks/jobpipe/blob/master/core/src/main/java/org/deephacks/jobpipe/Task.java) execution expressed in code.

![tools4j-cli](https://raw.github.com/deephacks/jobpipe/master/core/src/test/java/org/deephacks/jobpipe/dag.png)

```java
package org.deephacks.jobpipe;

public class TestPipeline implements Pipeline {

  @Override
  public void execute(PipelineContext context) {
    JobSchedule.newSchedule(context)
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
      .execute()
      .awaitFinish();
  }
}
```
This pipeline can be executed using the command line.

```bash
java -Djobpipe.cp=my-pipeline.jar -jar jobpipe-0.0.1-cli.jar TestPipeline -range 2015-01-14T10:00
```

The execution of this schedule may yield the following order of execution at exactly 2015-01-14T10:00. Tasks are scheduled immediately if the scheduled date have passed. Task execution is stalled until dependent tasks have valid output. Task 1, 4, 10, 12 may start in parallel.

```java
12, 11, 10, 9, 4, 6, 5, 1, 7, 3, 0, 8, 2
```
