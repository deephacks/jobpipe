package org.deephacks.jobpipe.spark;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.List;

public class SparkArgs {
  private static final String ARG_NAME = "jobpipe_spark_task_arg";
  private static final Gson gson = new Gson();

  public String appName;
  public String master;
  public String input;
  public String output;
  public List<String> dependencyOutput = new ArrayList<>();

  public SparkArgs() {
  }

  public static SparkArgs createFrom(String[] args) {
    return getArg(ARG_NAME, SparkArgs.class, args);
  }

  public SparkArgs(String appName, String master, String input, String output, List<String> dependencyOutput) {
    this.appName = appName;
    this.input = input;
    this.master = master;
    this.output = output;
    this.dependencyOutput = dependencyOutput;
  }

  public String[] toArgs(String[] args) {
    String[] newArgs = new String[args.length + 2];
    System.arraycopy(args, 0, newArgs, 0, args.length);
    newArgs[args.length] = "-" + ARG_NAME;
    newArgs[args.length + 1] = gson.toJson(this);
    return newArgs;
  }

  private static <T> T getArg(String arg, Class<T> cls, String[] args) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("-")) {
        String key = args[i].substring(1, args[i].length());
        if (key.equals(arg)) {
          return gson.fromJson(args[i + 1], cls);
        }
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return "SparkArgs{" +
      "appName='" + appName + '\'' +
      ", input='" + input + '\'' +
      ", output='" + output + '\'' +
      ", dependencyOutput=" + dependencyOutput +
      '}';
  }
}
