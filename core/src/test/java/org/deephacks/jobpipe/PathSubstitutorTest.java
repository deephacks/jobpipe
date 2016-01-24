package org.deephacks.jobpipe;

import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class PathSubstitutorTest {

  @Test
  public void testReplace() {
    String result = PathSubstitutor.newBuilder(new DateTime("2015-10-11T12:13"))
    .basePath("/tmp").replace();
    assertThat(result, is("/tmp/2015-10-11T12_13"));
  }

  @Test
  public void testReplaceMoreValues() {
    String result = PathSubstitutor.newBuilder(new DateTime("2015-10-11T12:13"))
      .pattern("${basePath}/${hour}/${seconds}")
      .sub("seconds", "59")
      .basePath("/tmp")
      .replace();
    assertThat(result, is("/tmp/12/59"));
  }
}
