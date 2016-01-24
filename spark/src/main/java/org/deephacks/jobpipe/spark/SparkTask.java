package org.deephacks.jobpipe.spark;

import org.apache.spark.launcher.SparkLauncher;
import org.deephacks.jobpipe.PathSubstitutor;
import org.deephacks.jobpipe.Task;
import org.deephacks.jobpipe.TaskContext;
import org.deephacks.jobpipe.TaskOutput;
import org.joda.time.DateTime;

import java.io.File;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SparkTask implements Task {
  protected SparkTaskBuilder builder;
  protected String appName;

  private SparkTask(SparkTaskBuilder builder) {
    this.builder = builder;
    this.appName = builder.getAppName();
  }

  public static SparkTaskBuilder newBuilder(Class<?> mainClass) {
    return new SparkTaskBuilder(mainClass);
  }

  @Override
  public void execute(TaskContext ctx) {
    try {
      SparkLauncher launcher = createLauncher();
      List<String> depOutput = ctx.getDependecyOutput().stream()
        .map(o -> o.get().toString())
        .collect(Collectors.toList());

      DateTime date = ctx.getTimeRange().from();
      String input = getInputPath(date);
      String output = getOutputPath(date);

      String[] args = new SparkArgs(builder.getAppName(), input, output, depOutput)
        .toArgs(ctx.getArgs());
      launcher.addAppArgs(args);
      Process process = launcher.launch();
      new Thread(new InputStreamThread(process.getInputStream(), System.out)).start();
      new Thread(new InputStreamThread(process.getErrorStream(), System.out)).start();
      process.waitFor();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TaskOutput getOutput(TaskContext context) {
    String path = getOutputPath(context.getTimeRange().from());
    return builder.output.apply(path);
  }

  private String getOutputPath(DateTime time) {
    return PathSubstitutor.newBuilder(time)
      .pattern(builder.outputPathPattern)
      .basePath(builder.basePath)
      .sub("appName", appName)
      .replace();
  }

  private String getInputPath(DateTime time) {
    return PathSubstitutor.newBuilder(time)
      .pattern(builder.inputPathPattern)
      .basePath(builder.basePath)
      .sub("appName", appName)
      .replace();
  }

  SparkLauncher createLauncher() {
    SparkLauncher launcher = new SparkLauncher();
    launcher.setMainClass(builder.mainClass.getName());
    if (builder.verbose != null) {
      launcher.setVerbose(builder.verbose);
    }
    launcher.setAppName(builder.getAppName());
    if (builder.appResource != null) {
      launcher.setAppResource(builder.appResource);
    } else {
      launcher.setAppResource(getJarAbsolutePath(builder.mainClass));
    }
    if (builder.deployMode != null) {
      launcher.setDeployMode(builder.deployMode);
    }
    if (builder.master != null) {
      launcher.setDeployMode(builder.master);
    }
    if (builder.propertiesFile != null) {
      launcher.setPropertiesFile(builder.propertiesFile);
    }
    if (builder.sparkHome != null) {
      launcher.setSparkHome(builder.sparkHome);
    }
    launcher.addJar(getJarAbsolutePath(Task.class));
    launcher.addJar(getJarAbsolutePath(SparkTask.class));
    builder.appArgs.forEach(arg -> launcher.addAppArgs(arg));
    builder.sparkArgs.forEach(arg -> launcher.addSparkArg(arg));
    builder.files.forEach(file -> launcher.addFile(file));
    builder.pyFiles.forEach(file -> launcher.addPyFile(file));
    builder.jars.stream().forEach(jar -> launcher.addJar(jar));
    builder.conf.forEach((k, v) -> launcher.setConf(k, v));
    return launcher;
  }

  public static class SparkTaskBuilder {
    Function<String, TaskOutput> output;
    String basePath;
    String outputPathPattern = "${basePath}/${appName}/${year}-${month}-${day}T${hour}";
    String inputPathPattern = "${basePath}/${appName}/${year}-${month}-${day}T${hour}";
    Boolean verbose;
    String appName;
    String appResource;
    String deployMode;
    Class<?> mainClass;
    String master;
    String sparkHome;
    protected String propertiesFile;
    final List<String> appArgs;
    final List<String> sparkArgs;
    final List<String> jars;
    final List<String> files;
    final List<String> pyFiles;
    final Map<String, String> conf;

    private SparkTaskBuilder(Class<?> mainClass) {
      this.appArgs = new ArrayList<>();
      this.sparkArgs = new ArrayList<>();
      this.conf = new HashMap<>();
      this.files = new ArrayList<>();
      this.jars = new ArrayList<>();
      this.pyFiles = new ArrayList<>();
      this.mainClass = mainClass;
    }

    public static SparkTaskBuilder newBuilder(Class<?> mainClass) {
      Objects.nonNull(mainClass);
      return new SparkTaskBuilder(mainClass);
    }

    public SparkTaskBuilder setOutputPattern(String outputPathPattern) {
      this.outputPathPattern = outputPathPattern;
      return this;
    }

    public SparkTaskBuilder setInputPattern(String inputPathPattern) {
      this.inputPathPattern = inputPathPattern;
      return this;
    }

    public SparkTaskBuilder setBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public SparkTaskBuilder setConfig(String name, String value) {
      conf.put(name, value);
      return this;
    }

    public SparkTaskBuilder addAppArgs(String... args) {
      this.appArgs.addAll(Arrays.asList(args));
      return this;
    }

    public SparkTaskBuilder setPropertiesFile(String path) {
      propertiesFile = path;
      return this;
    }

    public SparkTaskBuilder setDeployMode(String mode) {
      this.deployMode = mode;
      return this;
    }

    public SparkTaskBuilder addSparkArg(String value) {
      this.sparkArgs.add(value);
      return this;
    }

    public SparkTaskBuilder addFile(String file) {
      files.add(file);
      return this;
    }

    public SparkTaskBuilder setMaster(String master) {
      this.master = master;
      return this;
    }

    public SparkTaskBuilder addPyFile(String file) {
      pyFiles.add(file);
      return this;
    }

    public SparkTaskBuilder setSparkHome(String sparkHome) {
      this.sparkHome = sparkHome;
      return this;
    }

    public SparkTaskBuilder addJar(String jar) {
      this.jars.add(jar);
      return this;
    }

    public SparkTaskBuilder setVerbose(boolean verbose) {
      this.verbose = verbose;
      return this;
    }

    public SparkTaskBuilder setAppName(String appName) {
      this.appName = appName;
      return this;
    }

    public String getAppName() {
      return appName == null ? mainClass.getSimpleName() : appName;
    }

    public SparkTaskBuilder setAppResource(String resource) {
      this.appResource = resource;
      return this;
    }

    public SparkTask build() {
      return new SparkTask(this);
    }

    public SparkTaskBuilder setOutput(Function<String, TaskOutput> output) {
      this.output = output;
      return this;
    }
  }

  private static String getJarAbsolutePath(Class<?> cls) {
    return getJarPath(cls).getAbsolutePath();
  }

  private static File getJarPath(Class<?> cls) {
    File file = new File(cls.getProtectionDomain().getCodeSource().getLocation().getPath());
    return file;
  }
}
