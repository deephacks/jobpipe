package org.deephacks.jobpipe;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FileOutput implements TaskOutput {
  File file;

  public FileOutput() {
    try {
      this.file = Files.createTempDirectory("").toFile();
      this.file.delete();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void create() {
    try {
      file.createNewFile();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
