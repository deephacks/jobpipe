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
  private final ConcurrentLinkedDeque<ScheduledFuture<?>> scheduleHandles = new ConcurrentLinkedDeque<>();
  private final List<Node> schedule = new ArrayList<>();

  private JobSchedule(JobScheduleBuilder builder) {
    this.timeRange = builder.timeRange;
    this.tasks = builder.tasks;
  }

  public static JobScheduleBuilder newSchedule(PipelineContext context) {
    return new JobScheduleBuilder(context);
  }

  public static JobScheduleBuilder newSchedule(String timeFormat) {
    return new JobScheduleBuilder(timeFormat);
  }

  public static JobScheduleBuilder newSchedule(TimeRange range) {
    return new JobScheduleBuilder(range);
  }

  private List<Node> getJobSchedule(String taskId) {
    List<Node> nodes = tasks.values().stream()
      .flatMap(tasks -> tasks.stream())
      .collect(Collectors.toList());
    return getJobSchedule(nodes, taskId);
  }

  /**
   * Breadth first search.
   */
  private List<Node> getJobSchedule(List<Node> graph, String taskId) {
    ArrayList<Node> result = new ArrayList<>();
    if (graph == null || graph.size() == 0) {
      return graph;
    }
    if (taskId != null) {
      graph = createGraphFrom(taskId, graph);
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

  private List<Node> createGraphFrom(String taskId, List<Node> graph) {
    ArrayList<Node> result = new ArrayList<>();
    for (Node node : graph) {
      if (node.getId().equals(taskId)) {
        result.add(node);
        result.addAll(node.getDependencies());
      }
    }
    return result;
  }

  private List<Node> execute(String taskId) {
    List<Node> jobSchedule = getJobSchedule(taskId);
    if (jobSchedule.isEmpty()) {
      logger.info("Nothing to execute.");
      return new ArrayList<>();
    }
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
        if (dep.getStatus().hasFailed()) {
          logger.info("{} failed dependencies {}", node, dep);
          node.getStatus().failedDep();
          return;
        } else  if (!dep.hasOutput()) {
          retry(1);
          return;
        }
      }
      try {
        if (!node.hasOutput()) {
          node.getStatus().running();
          node.execute();
          node.getStatus().finished();
        } else {
          node.getStatus().skipped();
        }
      } catch (Throwable e) {
        node.getStatus().failed(e);
        logger.debug("Task execution failed " + node, e);
      }
    }

    public void retry(int sec) {
      logger.info("{} retry in {} seconds.", node, sec);
      ScheduledFuture<?> handle = node.getExecutor()
        .schedule(new ScheduleTask(node), sec, TimeUnit.SECONDS);
      node.getStatus().scheduled();
      scheduleHandles.add(handle);
    }

    public void schedule() {
      long timeout = node.getTimeout().getMillis() - System.currentTimeMillis();
      ScheduledFuture<?> handle = node.getExecutor()
        .schedule(this, timeout, TimeUnit.MILLISECONDS);
      node.getStatus().scheduled();
      scheduleHandles.add(handle);
    }
  }

  public static class JobScheduleBuilder {
    private TimeRange timeRange;
    private Map<String, List<Node>> tasks = new HashMap<>();
    private ScheduledThreadPoolExecutor defaultScheduler;
    private String taskId;
    private String[] args;

    private JobScheduleBuilder(String timeFormat) {
      this.timeRange = new TimeRange(timeFormat);
    }

    private JobScheduleBuilder(TimeRange range) {
      this.timeRange = range;
    }

    public JobScheduleBuilder(PipelineContext context) {
      this.timeRange = context.range;
      this.args = context.args;
      this.taskId = context.taskId;
    }

    public JobScheduleBuilder targetTask(Class<? extends Task> cls) {
      this.taskId = cls.getSimpleName();
      return this;
    }

    public JobScheduleBuilder targetTask(String taskId) {
      this.taskId = taskId;
      return this;
    }

    public TaskBuilder task(Class<? extends Task> cls) {
      return new TaskBuilder(cls, this);
    }

    public JobScheduleBuilder executor(ScheduledThreadPoolExecutor executor) {
      this.defaultScheduler = executor;
      return this;
    }

    public JobSchedule execute() {
      JobSchedule jobSchedule = new JobSchedule(this);
      jobSchedule.execute(taskId);
      new Thread(() -> {
        while (!jobSchedule.isFinished()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        jobSchedule.getJobSchedule(taskId).stream()
          .forEach(node -> node.getExecutor().shutdownNow());
      }).start();
      return jobSchedule;
    }
  }


  public static class TaskBuilder {
    private final Class<? extends Task> cls;
    private String id;
    private List<String> deps = new ArrayList<>();
    private TimeRangeType timeRangeType;
    private ScheduledThreadPoolExecutor executor;
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

    public TaskBuilder executor(ScheduledThreadPoolExecutor executor) {
      this.executor = executor;
      return this;
    }

    public JobScheduleBuilder add() {
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
      for (TimeRange range : timeRangeType.ranges(jobScheduleBuilder.timeRange)) {
        ScheduledThreadPoolExecutor executor = Optional.ofNullable(this.executor)
          .orElseGet(() -> jobScheduleBuilder.defaultScheduler = Optional.ofNullable(jobScheduleBuilder.defaultScheduler)
            .orElseGet(() -> new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors())));
        executor.setRemoveOnCancelPolicy(true);
        Node node = new Node(id, cls, range, executor, jobScheduleBuilder.args);
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


