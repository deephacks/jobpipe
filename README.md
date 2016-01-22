# jobpipe
Java scheduler for pipelines of long-running batch processes, inspired by Spotify [Luigi](https://github.com/spotify/luigi).

The purpose of jobpipe is to execute certain tasks at regular time intervals and allow expressing dependencies
between tasks as sequence of continuous executions in time. Every task produce output which is provided as input to dependent tasks. Any task is stalled until dependent task output is produced (if any). Idempotent task output enable resumability of pipelines that crash halfway. Task that fail (without a previously valid output) will transitively fail its dependent tasks.

The execution model of jobpipe is similar to that of a compiler, with the added dimension of time interval. 

### Example

Here's an abstract example of expressing a graph of task execution in code.

![tools4j-cli](https://raw.github.com/deephacks/jobpipe/master/core/src/test/java/org/deephacks/jobpipe/dag.png)

```java
    JobSchedule.newSchedule("2015-01-14T10:00")
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
```
