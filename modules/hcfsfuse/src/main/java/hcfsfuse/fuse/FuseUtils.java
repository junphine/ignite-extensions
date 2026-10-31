package hcfsfuse.fuse;

import alluxio.jnifuse.ErrorCodes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * FUSE and system user/group helper utilities used by HCFS-FUSE.
 * <p>
 * Replacement of {@code alluxio.fuse.AlluxioFuseUtils}: only the functionality needed by
 * HCFS-FUSE is kept, implemented without any Alluxio dependency. The user/group resolution
 * behaves identically to the original (see the command strings per OS).
 */
public final class FuseUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FuseUtils.class);

  /**
   * Most file systems on linux limit the length of file name beyond 255 characters.
   */
  public static final int MAX_NAME_LENGTH = 255;
  public static final long ID_NOT_SET_VALUE = -1;
  public static final long ID_NOT_SET_VALUE_UNSIGNED = 4294967295L;

  private static final String OS_NAME = System.getProperty("os.name").toLowerCase();

  private FuseUtils() {}

  /**
   * Invokes the given callable converting any throwable into an errno (-EIO).
   * Debug logs entry/exit and the execution duration.
   *
   * @param logger the logger used to output debug/error messages
   * @param callable operation to run
   * @param methodName the name of the executed method
   * @param format the format of the logged/passed parameters
   * @param args the arguments for format
   * @return the result of the callable, or -EIO when the callable threw
   */
  public static int call(Logger logger, FuseCallable callable,
      String methodName, String format, Object... args) {
    String params = logger.isDebugEnabled() ? String.format(format, args) : "";
    logger.debug("Enter: {}({})", methodName, params);
    long start = System.currentTimeMillis();
    int res;
    try {
      res = callable.call();
    } catch (Throwable t) {
      logger.error("Failed to {}({}) with unexpected throwable:", methodName, params, t);
      return -ErrorCodes.EIO();
    }
    long duration = System.currentTimeMillis() - start;
    logger.debug("Exit ({}): {}({}) in {} ms", res, methodName, params, duration);
    return res;
  }

  /**
   * A callable that returns an integer errno result and may throw.
   */
  @FunctionalInterface
  public interface FuseCallable {
    /**
     * Executes the operation.
     *
     * @return the result errno
     * @throws Exception any exception thrown during execution
     */
    int call() throws Exception;
  }

  /**
   * Converts a throwable into a negative errno.
   *
   * @param t the throwable to convert
   * @return the errno mapping of the throwable
   */
  public static int getErrorCode(Throwable t) {
    if (t instanceof IOException) {
      return -ErrorCodes.EIO();
    }
    return -ErrorCodes.EBADMSG();
  }

  /**
   * Gets the Uid of a user name.
   *
   * @param userName the userName to get the uid
   * @return the uid of the given user name
   */
  public static OptionalLong getUid(String userName) {
    return getIdInfo("-u", userName);
  }

  /**
   * Gets the Gid of a user name.
   *
   * @param userName the userName to get the gid
   * @return the gid of the given user name
   */
  public static OptionalLong getGidFromUserName(String userName) {
    return getIdInfo("-g", userName);
  }

  /**
   * Gets the gid of a group name.
   *
   * @param groupName the group name to get the gid
   * @return the gid of the given group name
   */
  public static OptionalLong getGidFromGroupName(String groupName) {
    try {
      String gidStr;
      if (isLinux()) {
        gidStr = execCommand("bash", "-c",
            String.format("getent group %s | cut -d: -f3", groupName)).trim();
      } else if (isMacOS()) {
        gidStr = execCommand("bash", "-c", String.format(
            "dscl . -read /Groups/%s | awk '($1 == \"PrimaryGroupID:\") { print $2 }'",
            groupName)).trim();
      } else {
        return OptionalLong.empty();
      }
      return OptionalLong.of(Long.parseLong(gidStr));
    } catch (NumberFormatException | IOException e) {
      LOG.error("Failed to get gid from group name {}.", groupName, e);
      return OptionalLong.empty();
    }
  }

  /**
   * Gets the user name of a uid.
   *
   * @param uid the uid to get user name
   * @return the user name of the given uid
   */
  public static Optional<String> getUserName(long uid) {
    if (uid == ID_NOT_SET_VALUE || uid == ID_NOT_SET_VALUE_UNSIGNED) {
      return Optional.empty();
    }
    try {
      String userName = execCommand("bash", "-c", String.format("id -nu %d", uid)).trim();
      return userName.isEmpty() ? Optional.empty() : Optional.of(userName);
    } catch (IOException e) {
      LOG.error("Failed to get user name of uid {}", uid, e);
      return Optional.empty();
    }
  }

  /**
   * Gets the group name of a gid.
   *
   * @param gid the gid to get group name
   * @return the group name of the given gid
   */
  public static Optional<String> getGroupName(long gid) {    if (gid == ID_NOT_SET_VALUE || gid == ID_NOT_SET_VALUE_UNSIGNED) {
      return Optional.empty();
    }
    try {
      String gidName;
      if (isLinux()) {
        gidName = execCommand("bash", "-c", String.format("getent group %s | cut -d: -f1", gid))
            .trim();
      } else if (isMacOS()) {
        gidName = execCommand("bash", "-c", String.format(
            "dscl . list /Groups PrimaryGroupID | awk '($2 == \"%d\") { print $1 }'", gid)).trim();
      } else {
        return Optional.empty();
      }
      return gidName.isEmpty() ? Optional.empty() : Optional.of(gidName);
    } catch (IOException e) {
      LOG.error("Failed to get group name of gid {}", gid, e);
      return Optional.empty();
    }
  }

  /**
   * Gets the primary group name of a user name.
   *
   * @param userName the user name to get group name
   * @return the group name of the given user name
   */
  public static Optional<String> getGroupName(String userName) {
    try {
      String group = execCommand("id", "-gn", userName).trim();
      if (!group.isEmpty()) {
        return Optional.of(group.split("\\s+")[0]);
      }
    } catch (IOException e) {
      LOG.warn("got exception trying to get groups for user {}: {}", userName, e.toString());
    }
    try {
      String groups = execCommand("id", "-Gn", userName).trim();
      if (!groups.isEmpty()) {
        return Optional.of(groups.split("\\s+")[0]);
      }
    } catch (IOException e) {
      LOG.warn("got exception trying to get groups for user {}: {}", userName, e.toString());
    }
    return Optional.empty();
  }

  /**
   * Validates the length of a path's name.
   *
   * @param name the name to check
   * @return true when the name is within the limit
   */
  public static boolean checkNameLength(String name) {
    return name.length() <= MAX_NAME_LENGTH;
  }

  /**
   * Gets "id -[u|g]" style info for a user.
   */
  private static OptionalLong getIdInfo(String idOption, String userName) {
    try {
      return OptionalLong.of(Long.parseLong(
          execCommand("id", idOption, userName).trim()));
    } catch (IOException | NumberFormatException e) {
      LOG.error("Failed to get id from {} with option {}", userName, idOption, e);
      return OptionalLong.empty();
    }
  }

  /**
   * Executes a command and returns its standard output (stderr merged in).
   *
   * @param args the command and its arguments
   * @return the command output
   * @throws IOException when the command exits with a non-zero exit code or fails to start
   */
  private static String execCommand(String... args) throws IOException {
    ProcessBuilder pb = new ProcessBuilder(args);
    pb.redirectErrorStream(true);
    Process process;
    try {
      process = pb.start();
    } catch (IOException e) {
      throw new IOException("Failed to run command: " + String.join(" ", args), e);
    }
    StringBuilder output = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (output.length() > 0) {
          output.append('\n');
        }
        output.append(line);
      }
    }
    int exitCode;
    try {
      exitCode = process.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      process.destroyForcibly();
      throw new IOException("Interrupted while running command: " + String.join(" ", args), e);
    }
    if (exitCode != 0) {
      throw new IOException(String.format(
          "Command %s failed with exit code %d, output:\n%s",
          String.join(" ", args), exitCode, output));
    }
    return output.toString();
  }

  private static boolean isLinux() {
    return OS_NAME.contains("linux");
  }

  private static boolean isMacOS() {
    return OS_NAME.contains("mac os");
  }
}
