# jobpipe
Java workflow scheduler for pipelines of long-running batch processes, inspired by Spotify [Luigi](https://github.com/spotify/luigi).

The purpose of jobpipe is to execute certain tasks at regular time ranges and allow expressing dependencies
between tasks as a sequence of continuous executions in time. These tasks can be anything, like Hadoop/Spark jobs, log data processing, data indexing or time series downsampling. 

- A schedule is a time dependent directed acyclic graph of task executions. 
- Tasks produce output which is provided as input to dependent tasks. 
- Tasks are stalled until their dependent tasks output are produced (if any). 
- Unrelated tasks in the graph succeed or fail independently.
- Failed tasks (without a previously valid output) fail its dependent tasks transitively. 
- Task output enable resumability of pipelines that crash halfway.
- There is no rollback mechanism. 
- Tasks may execute in parallel if their inputs are satisfied.

Unlike other workflow schedulers like Azkaban and Oozie, Jobpipe is a minimal library with zero dependencies where everything is expressed in code, no XML. Jobpipe tries hard to be unopinionated on how users should build their applications with regards to logging, configuration, monitoring, dependency injection, persistence, security, thread execution etc. Most of these concerns can be implemented by users as they see fit. 

[Download](http://search.maven.org/remotecontent?filepath=org/deephacks/jobpipe/jobpipe-cli/0.0.3/jobpipe-cli-0.0.3-capsule-fat.jar) the latest jobpipe release.

#### Example 1

Abstract [Pipeline](https://github.com/deephacks/jobpipe/blob/master/core/src/main/java/org/deephacks/jobpipe/Pipeline.java) of [Task](https://github.com/deephacks/jobpipe/blob/master/core/src/main/java/org/deephacks/jobpipe/Task.java) execution expressed in code.

![tools4j-cli](https://raw.github.com/deephacks/jobpipe/master/core/src/test/java/org/deephacks/jobpipe/dag.png)

```java
package org.deephacks.jobpipe;

public class TestPipeline implements Pipeline {

  @Override
  public void execute(PipelineContext context) {
    Task1 task = new Task1();
    JobSchedule.newSchedule(context)
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
      .execute()
      .awaitFinish();
  }
}
```
This pipeline can be executed using the command line.

```bash
java -jar jobpipe-cli.jar TestPipeline -range 2015-01-14T10:00
```

The execution of this schedule may yield the following order of execution at exactly 2016-01-14T10:00. Tasks are scheduled immediately if the scheduled date have passed. Task execution is stalled until dependent tasks have valid output. Task 1, 4, 10, 12 may start in parallel.

```java
12, 11, 10, 9, 4, 6, 5, 1, 7, 3, 0, 8, 2
```

#### Example 2 - Time ranges

Tasks can have different time ranges.

```java
    JobSchedule schedule = JobSchedule.newSchedule(context)
      .task(new Task1()).timeRange(HOUR).add()
      .task(new Task2()).timeRange(DAY).deps(Task1.class).add()
      .execute();
```

Executing this schedule for 2016-01-10 will yield the following task executions. Since the date have passed, the 'hourly' Task1 tasks may run in parallel and 'daily' Task2 afterwards.

```bash
[Task1,2016-01-10T23], [Task1,2016-01-10T22] ... [Task1,2016-01-10T01], [Task1,2016-01-10T00]
[Task2,2016-01-10]
```

A target time range can be sepecified as follows.

- Minute, 2016-01-10T10:10
- Hour, 2016-01-10T22
- Day, 2016-01-01
- Week, 2016-w01
- Month, 2016-01 
- Interval, 2016-10-10T10/2016-10-10T12 (2 hours) or 2016-10-10/2016-10-15 (5 days)

#### Example 3 - Arguments

Tasks accepts arguments that can be parsed with a library like [joptsimple](https://pholser.github.io/jopt-simple/).

```bash
java -jar jobpipe-cli.jar TestPipeline -range 2016-w01 -param1 value1
```

```java
public class ArgTask implements Task {

  @Override
  public void execute(TaskContext ctx) {
    String[] args = ctx.getArgs();
    OptionParser parser = new OptionParser();
    parser.allowsUnrecognizedOptions();
    OptionSpec<String> opt = parser.accepts("param1")
      .withRequiredArg().ofType(String.class).describedAs("param1");
    OptionSet options = parser.parse(args);
    if (options.has("param1")) {
      Object param1 = options.valueOf("param1");
    }
  }
}
```

#### Example 4 - Scheduling parallelism

Task execution parallelism can be controlled globally or individually using
[Scheduler](https://github.com/deephacks/jobpipe/blob/master/core/src/main/java/org/deephacks/jobpipe/Scheduler.java).

```java
    Scheduler globalMultiThreaded = new DefaultScheduler(10);
    Scheduler singleThreaded = new DefaultScheduler(1);
    
    JobSchedule.newSchedule(context)
      .scheduler(globalMultiThreaded)
      .task(new Task1()).add()
      .task(new Task2()).deps(Task1.class).scheduler(singleThreaded).add()
      .execute().awaitFinish();
```

#### Example 5 - Observers

Observers can be used to implement things like logging, monitoring, persistent history etc. Observers may also
reject task execution.

```java
public class JobObserverLog implements JobObserver {
  private static final Logger logger = LoggerFactory.getLogger(JobObserverLog.class);

  @Override
  public boolean notify(TaskStatus status) {
    logger.info("{} -> {}", status.getContext(), status.code());
    // if task execution should continue.
    return true;
  }
}
    
    JobSchedule.newSchedule(context)
      .observer(new JobObserverLog())
      .task(new Task1()).add()
      .task(new Task2()).deps(Task1.class).add()
      .execute().awaitFinish();
```

#### Example 6 - Command line

The command line jar provides a way for triggering a schedule at a certain time range, like 2016-01, 2013-W12, 2016-10-11 or 2013-12-01T12. Users can also choose to execute only single task through the ```-task``` option. Tasks are provided through user built jar files either in the ```/lib``` directory of the same directory as the command line jar and/or through the system property ```-Djobpipe.cp```. 

The following command run task 'task1' of the Pipeline class implementation that matches the regexp ```SparkPipeline``` and loads all files in the ```$SPARK_HOME/lib``` directory onto classpath (non-recursive).

```bash
export SPARK_HOME=/usr/local/spark-1.4.1-bin-hadoop2.6
export HADOOP_HOME=/usr/local/hadoop-2.7.1
export JOBPIPE_HOME=/usr/local/jobpipe
export MYPIPE_HOME=/usr/local/my-pipe
export HADOOP_CONF_DIR=/etc/hadoop/conf
export JOBPIPE_CP=$SPARK_HOME/lib:$MYPIPE_HOME:$JOBPIPE_HOME/spark/target/jobpipe-spark-0.0.4-SNAPSHOT.jar

java -Djobpipe.cp=$JOBPIPE_CP -jar $JOBPIPE_HOME/jobpipe-cli-0.0.3-capsule-fat.jar SparkPipeline -range 2016-01 -task task1
```

#### Example 7 - Apache Spark

Example of how to run Apache Spark pipelines are found in the [SparkPipeline](https://github.com/deephacks/jobpipe/blob/master/spark/src/test/java/org/deephacks/jobpipe/spark/SparkPipeline.java) test.
