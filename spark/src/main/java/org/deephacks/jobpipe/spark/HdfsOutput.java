package org.deephacks.jobpipe.spark;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.deephacks.jobpipe.*;

import java.io.IOException;

public class HdfsOutput implements TaskOutput {
  protected FileSystem fs;
  protected Path path;

  public HdfsOutput(String path, FileSystem fs) {
    this.fs = fs;
    this.path = new Path(path);
  }

  @Override
  public boolean exist() {
    try {
      return fs.exists(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object get() {
    return fs;
  }
}
