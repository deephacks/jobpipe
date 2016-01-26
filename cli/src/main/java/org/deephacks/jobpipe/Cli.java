package org.deephacks.jobpipe;

import joptsimple.*;

import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class Cli {

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();
    parser.formatHelpWith(new OptionFormatter());
    parser.allowsUnrecognizedOptions();

    OptionSpec<String> optRange = parser.accepts("range", "Date range for job to process, " +
      "ex 2016-01, 2013-W12, 2016-10-11, 2013-12-01T12")
      .withRequiredArg().ofType(String.class).describedAs("range");

    OptionSpec<String> optTaskId = parser.accepts("task", "Task to execute, or nothing for everything.")
      .withRequiredArg().ofType(String.class).describedAs("task");

    parser.accepts("h", "Display help");
    parser.accepts("v", "Print debug statements and exceptions.");

    OptionSpec<String> clsOpt = parser.nonOptions("Class regexp to run that implements a Pipeline " +
      "as a META-INF/services/org.deephacks.jobpipe.Pipeline")
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
    Boolean verbose = false;
    if (options.has("v")) {
      verbose = true;
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
    ArrayList<Pipeline> pipelines = new ArrayList<>();
    ServiceLoader.load(Pipeline.class).forEach(pipelines::add);
    Pattern pattern = Pattern.compile(cls);
    if (pipelines.isEmpty()) {
      System.out.println("No pipelines found. " +
        "Check that -Djobpipe.cp or lib classpath contains jar(s) with META-INF/services/org.deephacks.jobpipe.Pipeline");
    } else {
      for (Pipeline pipeline : pipelines) {
        if (pattern.matcher(pipeline.getClass().getName()).find()) {
          PipelineContext context = new PipelineContext(range, taskId, verbose, args);
          System.out.println("Executing " + pipeline.getClass().getName() + " for " + range);
          pipeline.execute(context);
          if (context.schedule != null) {
            context.schedule.awaitDone();
            System.out.println("\nFailure executing pipeline:");
            for (TaskStatus fail : context.schedule.getFailedTasks()) {
              System.out.println(fail.code() + " " + fail.getContext());
            }
          }
          return;
        }
      }
      System.out.println("'" + cls + "' matches no pipeline, existing pipelines:");
      pipelines.stream().map(p -> p.getClass().getName()).forEach(System.out::println);
    }
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
