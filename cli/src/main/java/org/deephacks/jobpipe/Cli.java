package org.deephacks.jobpipe;

import joptsimple.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Cli {

  public static void main(String[] args) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
    OptionParser parser = new OptionParser();
    parser.formatHelpWith(new OptionFormatter());
    parser.allowsUnrecognizedOptions();

    OptionSpec<String> optRange = parser.accepts("range", "Date range for job to process, " +
      "ex 2016-01, 2013-W12, 2016-10-11, 2013-12-01T12")
      .withRequiredArg().ofType(String.class).describedAs("range");

    OptionSpec<String> optTaskId = parser.accepts("task", "Task to execute, or nothing for everything.")
      .withRequiredArg().ofType(String.class).describedAs("task");

    parser.accepts("h", "Display help");

    OptionSpec<String> clsOpt = parser.nonOptions("Class to run the implements a Pipeline")
      .describedAs("cls");
    OptionSet options = parser.parse(args);
    List<String> argList = options.valuesOf(clsOpt);
    String cls = null;
    TimeRange range;
    String taskId = null;
    if (options.has("h")) {
      parser.printHelpOn(System.out);
      return;
    }
    if (argList != null && argList.size() > 0) {
      cls = argList.get(0);
    }
    if (cls == null) {
      System.out.println("Missing 'cls'");
      return;
    }
    if (options.has("range")) {
      range = new TimeRange(options.valueOf(optRange));
    } else {
      System.out.println("Missing 'range'");
      return;
    }
    if (options.has("task")) {
      taskId = options.valueOf(optTaskId);
    }
    Pipeline pipeline = (Pipeline) Class.forName(cls).newInstance();
    PipelineContext context = new PipelineContext(range, taskId);
    pipeline.execute(context);
  }

  static class OptionFormatter implements HelpFormatter {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public String format(Map<String, ? extends OptionDescriptor> options) {
      StringBuilder sb = new StringBuilder();
      sb.append("Usage: java -jar ... [options]");
      sb.append("\n");
      sb.append(" [opt] means optional argument.\n");
      sb.append(" <opt> means required argument.\n");
      sb.append(" \"+\" means comma-separated list of values.\n");
      sb.append("\n");
      for (OptionDescriptor each : options.values()) {
        sb.append(lineFor(each));
      }

      return sb.toString();
    }

    private String lineFor(OptionDescriptor d) {
      StringBuilder line = new StringBuilder();

      StringBuilder o = new StringBuilder();
      o.append("  ");
      for (String str : d.options()) {
        if (!d.representsNonOptions()) {
          o.append("-");
        }
        o.append(str);
        if (d.acceptsArguments()) {
          o.append(" ");
          if (d.requiresArgument()) {
            o.append("<");
          } else {
            o.append("[");
          }
          o.append(d.argumentDescription());
          if (d.requiresArgument()) {
            o.append(">");
          } else {
            o.append("]");
          }
        }
      }

      final int optWidth = 30;

      line.append(String.format("%-" + optWidth + "s", o.toString()));
      boolean first = true;
      String desc = d.description();
      List<?> defaults = d.defaultValues();
      if (defaults != null && !defaults.isEmpty()) {
        desc += " (default: " + defaults.toString() + ")";
      }
      for (String l : rewrap(desc)) {
        if (first) {
          first = false;
        } else {
          line.append(LINE_SEPARATOR);
          line.append(String.format("%-" + optWidth + "s", ""));
        }
        line.append(l);
      }

      line.append(LINE_SEPARATOR);
      line.append(LINE_SEPARATOR);
      return line.toString();
    }

    public static Collection<String> rewrap(String lines) {
      Collection<String> result = new ArrayList<String>();
      String[] words = lines.split("[ \n]");
      String line = "";
      int cols = 0;
      for (String w : words) {
        cols += w.length();
        line += w + " ";
        if (cols > 40) {
          result.add(line);
          line = "";
          cols = 0;
        }
      }
      if (!line.trim().isEmpty()) {
        result.add(line);
      }
      return result;
    }
  }

}
