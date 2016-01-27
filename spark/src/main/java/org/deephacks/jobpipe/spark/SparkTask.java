package org.deephacks.jobpipe.spark;

import org.apache.spark.launcher.SparkLauncher;
import org.deephacks.jobpipe.*;
import org.joda.time.DateTime;

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SparkTask implements Task {
  protected Builder config;

  private SparkTask(Builder config) {
    this.config = config.conclude();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(Builder builder) {
    return new Builder(builder);
  }

  @Override
  public void execute(TaskContext ctx) {
    Process process = null;
    try {
      DateTime date = ctx.getTimeRange().from();
      String input = getInputPath(date);
      String output = getOutputPath(date);

      SparkLauncher launcher = config.createLauncher(date);
      List<String> depOutput = ctx.getDependecyOutput().stream()
        .map(o -> o.get().toString())
        .collect(Collectors.toList());

      String[] args = new SparkArgs(config.appName, config.master, input, output, depOutput)
        .toArgs(ctx.getArgs());
      launcher.addAppArgs(args);
      process = launcher.launch();
      new Thread(new InputStreamThread(process.getInputStream(), System.out)).start();
      new Thread(new InputStreamThread(process.getErrorStream(), System.out)).start();
      process.waitFor();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (process != null && process.exitValue() != 0) {
        throw new RuntimeException("process exit status " + process.exitValue());
      }
    }
  }

  @Override
  public TaskSpec getTaskSpec() {
    TaskSpec taskSpec = config.mainClass.getAnnotation(TaskSpec.class);
    if (taskSpec == null) {
      taskSpec = new TaskSpec() {
        @Override
        public Class<? extends Annotation> annotationType() {
          return TaskSpec.class;
        }

        @Override
        public String id() {
          return config.mainClass.getSimpleName();
        }

        @Override
        public TimeRangeType timeRange() {
          return null;
        }
      };
    }
    return taskSpec;
  }

  @Override
  public TaskOutput getOutput(TaskContext context) {
    String path = getOutputPath(context.getTimeRange().from());
    return config.output.apply(path);
  }

  private String getOutputPath(DateTime time) {
    return PathSubstitutor.newBuilder(time)
      .pattern(config.outputPathPattern)
      .basePath(config.basePath)
      .sub("appName", config.appName)
      .replace();
  }

  private String getInputPath(DateTime time) {
    return PathSubstitutor.newBuilder(time)
      .pattern(config.inputPathPattern)
      .basePath(config.basePath)
      .sub("appName", config.appName)
      .replace();
  }


  public static class Builder {
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
    String propertiesFile;
    final List<String> appArgs;
    final List<String> sparkArgs;
    final List<String> jars;
    final List<String> files;
    final List<String> pyFiles;
    final Map<String, String> conf;

    public Builder() {
      this.appArgs = new ArrayList<>();
      this.sparkArgs = new ArrayList<>();
      this.conf = new HashMap<>();
      this.files = new ArrayList<>();
      this.jars = new ArrayList<>();
      this.pyFiles = new ArrayList<>();
    }

    public Builder(Builder builder) {
      this.appArgs = builder.appArgs;
      this.sparkArgs = builder.sparkArgs;
      this.conf = builder.conf;
      this.files = builder.files;
      this.jars = builder.jars;
      this.pyFiles = builder.pyFiles;
      this.sparkHome = builder.sparkHome;
      this.master = builder.master;
      this.output = builder.output;
      this.basePath = builder.basePath;
      this.outputPathPattern = builder.outputPathPattern;
      this.inputPathPattern = builder.inputPathPattern;
      this.verbose = builder.verbose;
      this.appName = builder.appName;
      this.appResource = builder.appResource;
      this.deployMode = builder.deployMode;
      this.mainClass = builder.mainClass;
    }

    public Builder mainClass(Class<?> cls) {
      this.mainClass = cls;
      return this;
    }

    public Builder appResource(String appResource) {
      this.appResource = appResource;
      return this;
    }

    public Builder outputPattern(String outputPathPattern) {
      this.outputPathPattern = outputPathPattern;
      return this;
    }

    public Builder inputPattern(String inputPathPattern) {
      this.inputPathPattern = inputPathPattern;
      return this;
    }

    public Builder basePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder putConfig(String name, String value) {
      conf.put(name, value);
      return this;
    }

    public Builder addAppArgs(String... args) {
      this.appArgs.addAll(Arrays.asList(args));
      return this;
    }

    public Builder propertiesFile(String path) {
      propertiesFile = path;
      return this;
    }

    public Builder deployMode(String mode) {
      this.deployMode = mode;
      return this;
    }

    public Builder addSparkArg(String value) {
      this.sparkArgs.add(value);
      return this;
    }

    public Builder addFile(String file) {
      files.add(file);
      return this;
    }

    public Builder master(String master) {
      this.master = master;
      return this;
    }

    public Builder addPyFile(String file) {
      pyFiles.add(file);
      return this;
    }

    public Builder sparkHome(String sparkHome) {
      this.sparkHome = sparkHome;
      return this;
    }

    public Builder addJar(String jar) {
      this.jars.add(jar);
      return this;
    }

    public Builder verbose(boolean verbose) {
      this.verbose = verbose;
      return this;
    }

    public Builder appName(String appName) {
      this.appName = appName;
      return this;
    }

    public Builder output(Function<String, TaskOutput> output) {
      this.output = output;
      return this;
    }

    public SparkTask build() {
      return new SparkTask(this);
    }

    SparkLauncher createLauncher(DateTime date) {
      SparkLauncher launcher = new SparkLauncher();
      launcher.setMainClass(mainClass.getName());
      if (verbose != null) {
        launcher.setVerbose(verbose);
      }
      launcher.setAppName(appName + "-" + date.toString("yyyy-MM-dd'T'HH:mm"));
      if (appResource != null) {
        launcher.setAppResource(appResource);
      } else {
        launcher.setAppResource(getJarAbsolutePath(mainClass));
      }
      if (deployMode != null) {
        launcher.setDeployMode(deployMode);
      }
      launcher.setMaster(master);
      if (propertiesFile != null) {
        launcher.setPropertiesFile(propertiesFile);
      }
      if (sparkHome != null) {
        launcher.setSparkHome(sparkHome);
      }
      launcher.addJar(getJarAbsolutePath(Task.class));
      launcher.addJar(getJarAbsolutePath(SparkTask.class));
      appArgs.forEach(arg -> launcher.addAppArgs(arg));
      // builder.sparkArgs.forEach(arg -> launcher.addSparkArg(arg));
      files.forEach(file -> launcher.addFile(file));
      pyFiles.forEach(file -> launcher.addPyFile(file));
      jars.stream().forEach(jar -> launcher.addJar(jar));
      conf.forEach((k, v) -> launcher.setConf(k, v));
      return launcher;
    }

    Builder conclude() {
      if (mainClass == null) {
        throw new NullPointerException("mainClass cannot be null");
      }
      appName = appName == null ? mainClass.getSimpleName() : appName;
      if (master == null) {
        master = "local";
      }
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
