package org.deephacks.jobpipe.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.deephacks.jobpipe.*;

import java.io.IOException;

public class HdfsOutput implements TaskOutput {
  protected FileSystem fs;
  protected Path path;

  public HdfsOutput(String path, Configuration conf) {
    try {
      this.fs = FileSystem.get(conf);
      this.path = new Path(conf.get("fs.defaultFS"), path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean exist() {
    try {
      return fs.exists(new Path(path, "_SUCCESS"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object get() {
    return path;
  }
}
