package org.deephacks.jobpipe;

import org.joda.time.DateTime;
import org.joda.time.IllegalFieldValueException;
import org.junit.Test;


import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TimeRangeTest {

  @Test(expected = IllegalArgumentException.class)
  public void testUnknownFormat() {
    new TimeRange("2015 10 11");
  }

  @Test
  public void testHour() {
    TimeRange t = new TimeRange("2015-10-11T11");
    assertThat(t.next().from(), is(new DateTime("2015-10-11T12")));
    assertThat(t.prev().from(), is(new DateTime("2015-10-11T10")));

    assertThat(t.hours().size(), is(1));
    assertThat(t.hours().get(0), is(new DateTime("2015-10-11T11")));

    assertThat(t.days().size(), is(0));
  }

  @Test(expected = IllegalFieldValueException.class)
  public void testInvalidHourFormat() {
    new TimeRange("2015-10-11T25");
  }


  @Test
  public void testDay() {
    TimeRange t = new TimeRange("2015-10-11");
    assertThat(t.next().from(), is(new DateTime("2015-10-12")));
    assertThat(t.prev().from(), is(new DateTime("2015-10-10")));

    assertThat(t.hours().size(), is(24));
    assertThat(t.hours().get(0), is(new DateTime("2015-10-11T00:00")));
    assertThat(t.hours().get(23), is(new DateTime("2015-10-11T23:00")));

    assertThat(t.days().size(), is(1));
    assertThat(t.days().get(0), is(new DateTime("2015-10-11")));
  }

  @Test
  public void testWeek() {
    TimeRange t = new TimeRange("2015-W12");
    assertThat(t.from(), is(new DateTime("2015-03-16")));
    assertThat(t.next().from(), is(new DateTime("2015-03-23")));
    assertThat(t.prev().from(), is(new DateTime("2015-03-09")));

    assertThat(t.hours().size(), is(24 * 7));
    assertThat(t.hours().get(0), is(new DateTime("2015-03-16T00:00")));
    assertThat(t.hours().get(24 * 7 - 1), is(new DateTime("2015-03-22T23:00")));

    assertThat(t.days().size(), is(7));
    assertThat(t.days().get(0), is(new DateTime("2015-03-16T00:00")));
    assertThat(t.days().get(6), is(new DateTime("2015-03-22")));
  }


  @Test
  public void testMonth() {
    TimeRange t = new TimeRange("2015-11");
    assertThat(t.next().from(), is(new DateTime("2015-12")));
    assertThat(t.prev().from(), is(new DateTime("2015-10")));

    assertThat(t.hours().size(), is(24 * 30));
    assertThat(t.hours().get(0), is(new DateTime("2015-11-01T00:00")));
    assertThat(t.hours().get(24 * 30 - 1), is(new DateTime("2015-11-30T23:00")));

    assertThat(t.days().size(), is(30));
    assertThat(t.days().get(0), is(new DateTime("2015-11-01")));
    assertThat(t.days().get(29), is(new DateTime("2015-11-30")));
  }

}
