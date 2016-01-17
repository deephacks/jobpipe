package org.deephacks.jobpipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class JobSchedule {
  private static final Logger logger = LoggerFactory.getLogger(JobSchedule.class);
  private final TimeRange timeRange;
  private final Map<String, List<Node>> tasks;
  private final Properties properties;
  private final ConcurrentLinkedDeque<ScheduledFuture<?>> scheduleHandles = new ConcurrentLinkedDeque<>();
  private final List<Node> schedule = new ArrayList<>();

  private JobSchedule(JobScheduleBuilder builder) {
    this.timeRange = builder.timeRange;
    this.properties = Optional.ofNullable(builder.properties).orElse(new Properties());
    this.tasks = builder.tasks;
  }

  public static JobScheduleBuilder newSchedule(String timeFormat) {
    return new JobScheduleBuilder(timeFormat);
  }

  private List<Node> getJobSchedule() {
    List<Node> nodes = tasks.values().stream()
      .flatMap(tasks -> tasks.stream())
      .collect(Collectors.toList());
    return getJobSchedule(nodes);
  }

  /**
   * Breadth first search.
   */
  private List<Node> getJobSchedule(List<Node> graph) {
    ArrayList<Node> result = new ArrayList<>();
    if (graph == null || graph.size() == 0) {
      return graph;
    }
    // keep track of all neighbours
    HashMap<Node, Integer> neighbours = new HashMap<>();
    for (Node node : graph) {
      for (Node neighbour : node.getDirectDependencies()) {
        neighbours.compute(neighbour, (key, val) -> val == null ? 1 : val + 1);
      }
    }

    // add root nodes
   Queue<Node> queue = new LinkedList<>();
    for (Node node : graph) {
      if (!neighbours.containsKey(node)) {
        queue.offer(node);
        result.add(node);
      }
    }
    // go through all children
    while (!queue.isEmpty()) {
      Node node = queue.poll();
      for (Node n : node.getDirectDependencies()) {
        neighbours.put(n, neighbours.get(n) - 1);
        if (neighbours.get(n) == 0) {
          result.add(n);
          queue.offer(n);
        }
      }
    }
    Collections.reverse(result);
    return result;
  }

  private List<Node> execute() {
    List<Node> jobSchedule = getJobSchedule();
    schedule.addAll(jobSchedule);
    for (Node n : jobSchedule) {
      new ScheduleTask(n).schedule();
    }
    return jobSchedule;
  }

  public boolean isFinished() {
    for (ScheduledFuture<?> handle : scheduleHandles) {
      if (!handle.isDone()) {
        return false;
      }
    }
    return true;
  }

  public void awaitFinish() {
    while (!isFinished()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public List<Task> getScheduledTasks() {
    return schedule.stream().map(node -> node.getTask()).collect(Collectors.toList());
  }

  private class ScheduleTask implements Runnable {
    Node node;

    public ScheduleTask(Node node) {
      this.node = node;
    }

    @Override
    public void run() {
      for (Node dep : node.getDependencies()) {
        if (!dep.isFinished()) {
          ScheduledFuture<?> handle = node.getExecutor()
            .schedule(new ScheduleTask(node), 1, TimeUnit.SECONDS);
          scheduleHandles.add(handle);
          return;
        }
      }
      if (!node.isFinished()) {
        logger.info("Executing {}", node);
        try {
          node.execute();
        } catch (Throwable e) {
          logger.warn("Task execution failed.", e);
          logger.info("Reschedule task {} in {} seconds.", node);
          ScheduledFuture<?> handle = node.getExecutor()
            .schedule(new ScheduleTask(node), Config.FAILED_TASK_RETRY_SEC, TimeUnit.SECONDS);
          scheduleHandles.add(handle);
        }
      } else {
        logger.info("Skipping  {}", node);
      }
    }

    public void schedule() {
      logger.info("Schedule task {}", node.getTask());
      long timeout = node.getTimeout().getMillis() - System.currentTimeMillis();
      ScheduledFuture<?> handle = node.getExecutor()
        .schedule(this, timeout, TimeUnit.MILLISECONDS);
      scheduleHandles.add(handle);
    }
  }

  public static class JobScheduleBuilder {
    private TimeRange timeRange;
    private Map<String, List<Node>> tasks = new HashMap<>();
    private Properties properties;
    private ScheduledExecutorService defaultScheduler;

    private JobScheduleBuilder(String timeFormat) {
      this.timeRange = new TimeRange(timeFormat);
    }

    public TaskBuilder task(Class<? extends Task> cls) {
      return new TaskBuilder(cls, this);
    }

    public JobScheduleBuilder executor(ScheduledExecutorService executor) {
      this.defaultScheduler = executor;
      return this;
    }

    public JobScheduleBuilder properties(Properties properties) {
      this.properties = properties;
      return this;
    }

    public JobSchedule execute() {
      JobSchedule jobSchedule = new JobSchedule(this);
      jobSchedule.execute();
      return jobSchedule;
    }
  }

  public static class TaskBuilder {
    private final Class<? extends Task> cls;
    private String id;
    private List<String> deps = new ArrayList<>();
    private TimeRangeType timeRangeType;
    private ScheduledExecutorService executor;
    private JobScheduleBuilder jobScheduleBuilder;

    private TaskBuilder(Class<? extends Task> cls, JobScheduleBuilder jobScheduleBuilder) {
      this.cls = cls;
      this.jobScheduleBuilder = jobScheduleBuilder;
    }

    public TaskBuilder id(String id) {
      this.id = id;
      return this;
    }

    public TaskBuilder depIds(String... ids) {
      depIds(Arrays.asList(ids));
      return this;
    }

    public TaskBuilder depIds(Collection<String> ids) {
      this.deps.addAll(ids);
      return this;
    }

    public TaskBuilder deps(Collection<Class<? extends Task>> tasks) {
      List<String> ids = tasks.stream()
        .map(cls -> cls.getSimpleName())
        .collect(Collectors.toList());
      return depIds(ids);
    }

    public TaskBuilder deps(Class<? extends Task>... tasks) {
      return deps(Arrays.asList(tasks));
    }

    public TaskBuilder timeRange(TimeRangeType type) {
      this.timeRangeType = type;
      return this;
    }

    public TaskBuilder executor(ScheduledExecutorService executor) {
      this.executor = executor;
      return this;
    }

    public JobScheduleBuilder addTask() {
      TaskSpec taskSpec = cls.getAnnotation(TaskSpec.class);
      if (taskSpec != null) {
        if (timeRangeType == null) {
          timeRangeType = taskSpec.timeRange();
        }
        if (id != null && !taskSpec.id().isEmpty()) {
          id = taskSpec.id();
        }
      }
      if (id == null) {
        id = cls.getSimpleName();
      }
      if (jobScheduleBuilder.timeRange.getType().ordinal() < timeRangeType.ordinal()) {
        throw new IllegalArgumentException("Target job time range " + jobScheduleBuilder.timeRange +
          " is less than task time range for " + id + ", so it will never complete.");
      }
      for (TimeRange range : timeRangeType.ranges(jobScheduleBuilder.timeRange)) {
        ScheduledExecutorService executor = Optional.ofNullable(this.executor)
          .orElseGet(() -> jobScheduleBuilder.defaultScheduler = Optional.ofNullable(jobScheduleBuilder.defaultScheduler)
            .orElseGet(() -> Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors())));
        Node node = new Node(id, cls, range, jobScheduleBuilder.properties, executor);
        for (String dep : deps) {
          List<Node> nodes = jobScheduleBuilder.tasks.get(dep);
          if (nodes == null) {
            throw new IllegalArgumentException("Dependency does not exist " + dep);
          }
          for (Node n : nodes) {
            node.addDependencies(n);
          }
        }

        List<Node> nodes = jobScheduleBuilder.tasks.computeIfAbsent(id, key -> new ArrayList<>());
        if (nodes.contains(node)) {
          throw new IllegalArgumentException(node.getTask() + " already exist");
        }
        nodes.add(node);
      }
      return jobScheduleBuilder;
    }
  }
}


