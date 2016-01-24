package org.deephacks.jobpipe.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.deephacks.jobpipe.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.deephacks.jobpipe.TimeRangeType.HOUR;

public class SparkPipeline implements Pipeline {

  public static void main(String[] args) {
    SparkTask task = SparkTask.newBuilder(SparkTask1.class)
      .setBasePath("/tmp")
      .setMaster("local")
      .setInputPattern("${basePath}/files")
      .setOutput(LocalOutput::new)
      .build();
    JobSchedule.newSchedule("2015-10-11T11")
      .task(task).timeRange(HOUR).add()
      .execute().awaitFinish().shutdownAfter();

  }

  @Override
  public void execute(PipelineContext context) {
    SparkTask task = SparkTask.newBuilder(SparkTask1.class)
      .setInputPattern("/tmp/files")
      .build();
    SparkTask.newBuilder(SparkTask1.class).build();
    JobSchedule.newSchedule(context)
      .task(task).timeRange(HOUR).add()
      .execute().awaitFinish().shutdownAfter();
  }


  public static class LocalOutput implements TaskOutput {
    File file;

    public LocalOutput(String path) {
      this.file = new File(path);
    }

    @Override
    public boolean exist() {
      return file.exists();
    }

    @Override
    public Object get() {
      return file;
    }
  }

  public static class SparkTask1 {

    public static void main(String[] args) {
      System.out.println(Arrays.asList(args));
      try {
        SparkArgs sparkArgs = SparkArgs.createFrom(args);
        List<String> lines = Files.list(Paths.get(sparkArgs.input))
          .flatMap(SparkTask1::lines)
          .collect(Collectors.toList());
        new JavaSparkContext(sparkArgs.getSparkConf())
          .parallelize(lines)
          .saveAsTextFile(sparkArgs.output);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public static Stream<String> lines(Path p) {
      try {
        return Files.readAllLines(p).stream();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
