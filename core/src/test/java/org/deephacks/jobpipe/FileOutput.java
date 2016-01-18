package org.deephacks.jobpipe;

import java.io.File;

public class FileOutput implements TaskOutput {
  File file;

  public FileOutput(File file) {
    this.file = file;
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
