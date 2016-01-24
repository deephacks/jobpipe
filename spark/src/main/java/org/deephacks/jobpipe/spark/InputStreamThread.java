package org.deephacks.jobpipe.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class InputStreamThread implements Runnable {
  InputStream inputStream;
  OutputStream output;

  public InputStreamThread(InputStream inputStream, OutputStream output) {
    this.inputStream = inputStream;
    this.output = output;
  }

  @Override
  public void run() {
    try {
      int n;
      byte[] buffer = new byte[1024];
      while ((n = inputStream.read(buffer)) > -1) {
        output.write(buffer, 0, n);
        output.flush();
      }
    } catch (final Exception e) {
      try {
        inputStream.close();
      } catch (IOException e1) {
      }
      try {
        output.close();
      } catch (Exception e1) {
      }
   }
  }
}
