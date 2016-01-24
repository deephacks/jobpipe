package org.deephacks.jobpipe;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PathSubstitutor {
  private static final String DEFAULT_PATTERN = "${basePath}/${year}-${month}-${day}T${hour}_${minute}";

  public static Builder newBuilder(DateTime dateTime) {
    return new Builder(dateTime);
  }

  public static class Builder {
    private String pattern;
    private String basePath;
    private HashMap<String, String> values = new HashMap<>();
    private DateTime dateTime;

    public Builder(DateTime dateTime) {
      this.dateTime = dateTime;
    }

    public Builder pattern(String pattern) {
      this.pattern = pattern;
      return this;
    }

    public Builder basePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder sub(String key, String value) {
      this.values.put(key, value);
      return this;
    }

    public String replace() {
      if (pattern == null) {
        this.pattern = DEFAULT_PATTERN;
      }
      if (basePath == null) {
        this.basePath = "/tmp";
      }
      return PathSubstitutor.replace(pattern, basePath, dateTime, values);
    }
  }

  private static String replace(String basePath, DateTime time) {
    return replace(DEFAULT_PATTERN, basePath, time, new HashMap<>());
  }

  private static String replace(String pattern, String basePath, DateTime time) {
    return replace(pattern, basePath, time, new HashMap<>());
  }

  public static String replace(String pattern, String basePath, DateTime time, HashMap<String, String> values) {
    HashMap<String, String> map = new HashMap<>();
    map.putAll(values);
    map.put("basePath", basePath);
    map.put("year", String.valueOf(time.getYear()));
    map.put("month", String.format("%02d", time.getMonthOfYear()));
    map.put("day", String.format("%02d", time.getDayOfMonth()));
    map.put("hour", String.format("%02d", time.getHourOfDay()));
    map.put("minute", String.format("%02d", time.getMinuteOfHour()));
    return replace(pattern, map);
  }

  private static String replace(String pattern, HashMap<String, String> map) {
    String result = pattern;
    for (String var : parseVars(pattern)) {
      result = replaceVar(var, map.get(var), result);
    }
    return result;
  }

  private static List<String> parseVars(String pattern) {
    List<String> names = new ArrayList<>();
    int pos = 0, max = pattern.length();
    while (pos < max) {
      pos = pattern.indexOf("${", pos);
      if (pos == -1) {
        break;
      }
      int end = pattern.indexOf('}', pos + 2);
      if (end == -1) {
        break;
      }
      String name = pattern.substring(pos + 2, end);
      names.add(name);
      pos = end + 1;
    }
    return names;
  }

  private static String replaceVar(String key, String value, String text) {
    if (value != null) {
      return text.replaceAll("\\$\\{" + key + "}", value);
    }
    return text;
  }
}
