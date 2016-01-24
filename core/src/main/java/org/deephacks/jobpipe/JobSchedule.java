package org.deephacks.jobpipe;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class JobSchedule {
  private final TimeRange timeRange;
  private final Map<String, List<Node>> tasks;
  private final List<Node> schedule = new ArrayList<>();

  private JobSchedule(JobScheduleBuilder builder) {
    this.timeRange = builder.timeRange;
    this.tasks = builder.tasks;
  }

  public static JobScheduleBuilder newSchedule(PipelineContext context) {
    return new JobScheduleBuilder(context);
  }

  /**
   * @param timeFormat the time range that this schedule should execute.
   */
  public static JobScheduleBuilder newSchedule(String timeFormat) {
    return new JobScheduleBuilder(timeFormat);
  }

  /**
   * @param range the time range that this schedule should execute.
   */
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
      return new ArrayList<>();
    }
    schedule.addAll(jobSchedule);
    for (Node n : jobSchedule) {
      new ScheduleTask(n).schedule();
    }
    return jobSchedule;
  }

  /**
   * @return all tasks are finished executing.
   */
  public boolean isFinished() {
    for (TaskStatus status : getScheduledTasks()) {
      if (!status.isDone()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Waits until all tasks are finished executing.
   */
  public JobSchedule awaitFinish() {
    while (!isFinished()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return this;
  }

  public void shutdownAfter() {
    for (TaskStatus status : getScheduledTasks()) {
      try {
        status.getContext().node.getScheduler().shutdown();
      } catch (Exception e) {
      }
    }
  }

  /**
   * @return all tasks that have been scheduled, including finished tasks.
   */
  public List<TaskStatus> getScheduledTasks() {
    return schedule.stream().map(node -> node.getStatus()).collect(Collectors.toList());
  }

  private class ScheduleTask implements Runnable {
    Node node;

    ScheduleTask(Node node) {
      this.node = node;
    }

    @Override
    public void run() {
      for (Node dep : node.getDependencies()) {
        if (dep.getStatus().hasFailed()) {
          node.getStatus().failedDep(dep.getContext());
          return;
        } else if (!dep.hasOutput()) {
          retry(1);
          return;
        }
      }
      try {
        if (!node.hasOutput()) {
          if (node.getStatus().running()) {
            node.execute();
            node.getStatus().finished();
          }
        } else {
          node.getStatus().skipped();
        }
      } catch (Throwable e) {
        node.getStatus().failed(e);
      }
    }

    void retry(int sec) {
      if (node.getStatus().scheduled()) {
        node.getScheduler().schedule(new ScheduleTask(node), sec, TimeUnit.SECONDS);
      }
    }

    void schedule() {
      if (node.getStatus().scheduled()) {
        long timeout = node.getTimeout().getMillis() - System.currentTimeMillis();
        node.getScheduler().schedule(this, timeout, TimeUnit.MILLISECONDS);
      }
    }
  }

  public static class JobScheduleBuilder {
    private TimeRange timeRange;
    private Map<String, List<Node>> tasks = new HashMap<>();
    private Scheduler defaultScheduler;
    private JobObserver observer;
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


    /**
     * @param task create a new task
     */
    public TaskBuilder task(Task task) {
      return new TaskBuilder(task, this);
    }

    /**
     * @param cls start executing at this task, including dependent tasks.
     */
    public JobScheduleBuilder targetTask(Class<? extends Task> cls) {
      this.taskId = cls.getSimpleName();
      return this;
    }

    /**
     * @param taskId start execution at this task, including dependent tasks.
     */
    public JobScheduleBuilder targetTask(String taskId) {
      this.taskId = taskId;
      return this;
    }

    /**
     * @param observer will get notified when tasks transitions into a new state.
     */
    public JobScheduleBuilder observer(JobObserver observer) {
      this.observer = observer;
      return this;
    }

    /**
     * @param scheduler the default scheduler to use for scheduling of tasks.
     */
    public JobScheduleBuilder scheduler(Scheduler scheduler) {
      this.defaultScheduler = scheduler;
      return this;
    }

    public JobSchedule execute() {
      JobSchedule jobSchedule = new JobSchedule(this);
      jobSchedule.execute(taskId);
      return jobSchedule;
    }
  }


  public static class TaskBuilder {
    private final Task task;
    private String id;
    private List<String> deps = new ArrayList<>();
    private TimeRangeType timeRangeType;
    private Scheduler scheduler;
    private JobScheduleBuilder jobScheduleBuilder;

    private TaskBuilder(Task task, JobScheduleBuilder jobScheduleBuilder) {
      this.task = task;
      this.jobScheduleBuilder = jobScheduleBuilder;
    }

    /**
     * The id of this tasks. Defaults to the task simple classname if not explicitly
     * set by {@link org.deephacks.jobpipe.TaskSpec}.
     */
    public TaskBuilder id(String id) {
      this.id = id;
      return this;
    }

    /**
     * @param ids dependent tasks
     */
    public TaskBuilder depIds(String... ids) {
      depIds(Arrays.asList(ids));
      return this;
    }

    /**
     * @param ids dependent tasks
     */
    public TaskBuilder depIds(Collection<String> ids) {
      this.deps.addAll(ids);
      return this;
    }

    /**
     * @param tasks dependent tasks
     */
    public TaskBuilder deps(Collection<Class<? extends Task>> tasks) {
      List<String> ids = tasks.stream()
        .map(cls -> cls.getSimpleName())
        .collect(Collectors.toList());
      return depIds(ids);
    }

    /**
     * @param tasks dependent tasks
     */
    public TaskBuilder deps(Class<? extends Task>... tasks) {
      return deps(Arrays.asList(tasks));
    }

    /**
     * Defaults to {@link org.deephacks.jobpipe.TaskSpec} if set.
     *
     * @param type the time range that the task operates on.
     */
    public TaskBuilder timeRange(TimeRangeType type) {
      this.timeRangeType = type;
      return this;
    }

    /**
     * @param scheduler override the default scheduler for this task.
     */
    public TaskBuilder scheduler(Scheduler scheduler) {
      this.scheduler = scheduler;
      return this;
    }

    /**
     * Adds this task to the schedule.
     */
    public JobScheduleBuilder add() {
      TaskSpec taskSpec = task.getClass().getAnnotation(TaskSpec.class);
      if (taskSpec != null) {
        if (timeRangeType == null) {
          timeRangeType = taskSpec.timeRange();
        }
        if (id != null && !taskSpec.id().isEmpty()) {
          id = taskSpec.id();
        }
      }
      if (id == null) {
        id = task.getClass().getSimpleName();
      }
      if (timeRangeType == null) {
        throw new IllegalArgumentException(id + " does not have a time range.");
      }
      for (TimeRange range : timeRangeType.ranges(jobScheduleBuilder.timeRange)) {
        Scheduler scheduler = Optional.ofNullable(this.scheduler)
          .orElseGet(() -> jobScheduleBuilder.defaultScheduler = Optional.ofNullable(jobScheduleBuilder.defaultScheduler)
            .orElseGet(() -> new DefaultScheduler()));
        Node node = new Node(id, task, range, scheduler, jobScheduleBuilder.args, jobScheduleBuilder.observer);
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
