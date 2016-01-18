import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Extends capsule classpath by first adding 'lib' directory in same directory as jar,
 * and also enables user to specify additional directories using 'jobpipe.cp' env prop:
 * java -Djobpipe.cp=/tmp/cp1:/tmp/cp2:$HADOOP_CONF_DIR -jar jobpipe-0.0.1-SNAPSHOT-cli.jar
 */
public class JobPipeCapsule extends Capsule {
  protected JobPipeCapsule(Capsule pred) {
    super(pred);
  }

  @Override
  protected <T> T attribute(Map.Entry<String, T> attr) {
    if (attr == ATTR_APP_CLASS_PATH) {
      final List<Object> args = new ArrayList<>(super.attribute(ATTR_APP_CLASS_PATH));
      ArrayList<String> dirs = new ArrayList<>();
      dirs.add(new File(getJarFile().toFile().getParent(), "lib").getAbsolutePath());
      String cpStr = System.getProperty("jobpipe.cp");
      if (cpStr != null && !cpStr.isEmpty()) {
        List<String> cps = Arrays.asList(cpStr.split(":"));
        for (String cp : cps) {
          if (cp.startsWith("$")) {
            String property = System.getProperty(cp.substring(1, cp.length()));
            dirs.add(property);
          } else {
            dirs.add(cp);
          }
        }
      }
      for (String dir : dirs) {
        File file = new File(dir);
        if (file.exists() && file.isDirectory()) {
          for (File f : file.listFiles()) {
            if (!f.isDirectory()) {
              args.add(f.getAbsolutePath());
            }
          }
        } else if (file.exists()) {
          args.add(file.getAbsolutePath());
        }
      }
      return (T) args;
    }
    return super.attribute(attr);
  }
}
