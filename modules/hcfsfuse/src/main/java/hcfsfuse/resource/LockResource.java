package hcfsfuse.resource;

import java.util.concurrent.locks.Lock;

/**
 * Class for representing and acquiring and releasing a lock. This enables try-with-resources
 * usage for acquiring lock and releasing lock.
 * <p>
 * Replacement of {@code alluxio.resource.LockResource}.
 */
public class LockResource implements AutoCloseable {
  private final Lock mLock;
  private final long acquiredMs;

  /**
   * Creates a new instance of {@link LockResource}. This acquires the lock and keep it until
   * the resource is closed.
   *
   * @param lock the lock
   */
  public LockResource(Lock lock) {
    mLock = lock;
    acquiredMs = System.currentTimeMillis();
    mLock.lock();
  }

  /**
   * Returns how long the lock has been held.
   *
   * @return the duration in milliseconds the lock has been held
   */
  public boolean isLockHeld() {
    return mLock != null;
  }

  /**
   * Returns the time in milliseconds when the lock was acquired.
   *
   * @return time in milliseconds when the lock was acquired
   */
  public long getAcquiredMs() {
    return acquiredMs;
  }

  @Override
  public void close() {
    mLock.unlock();
  }
}
