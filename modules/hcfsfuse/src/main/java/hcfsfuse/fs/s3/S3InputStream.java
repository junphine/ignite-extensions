package hcfsfuse.fs.s3;

import org.apache.hadoop.fs.FSInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;

/**
 * Hadoop {@link org.apache.hadoop.fs.FSInputStream} reading data directly from S3 (or any
 * S3-compatible object store) with AWS SDK v2.
 * <p>
 * Sequential reads are served through an internal read-ahead buffer so that FUSE-sized reads
 * result in far fewer remote requests, while random access seeks simply recompute the absolute
 * range within the buffer.
 */
public final class S3InputStream extends FSInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3InputStream.class);

  private final S3Client mClient;
  private final String mBucket;
  private final String mKey;
  private final long mContentLength;
  private final int mBufferSize;

  /** Absolute position of the next byte to be returned. */
  private long mPos;
  /** Absolute position of the first byte held in the buffer. */
  private long mBufferOffset;
  /** Number of valid bytes in the buffer. */
  private int mBufferLen = 0;
  private byte[] mBuffer = new byte[0];
  /** [FIX-5] volatile so non-synchronized positioned reads see close() promptly. */
  private volatile boolean mClosed = false;

  /**
   * Creates a new instance of {@link S3InputStream}.
   *
   * @param client the s3 client
   * @param bucket the bucket name
   * @param key the key of the object
   * @param contentLength the size of the object
   * @param bufferSize read-ahead buffer size in bytes
   */
  public S3InputStream(S3Client client, String bucket, String key, long contentLength,
                       int bufferSize) {
    if (contentLength < 0) {
      throw new IllegalArgumentException("negative content length for " + key);
    }
    mClient = client;
    mBucket = bucket;
    mKey = key;
    mContentLength = contentLength;
    mBufferSize = Math.max(1, bufferSize);
  }

  /**
   * Validates the standard {@code (buf, off, len)} argument triplet.
   * [FIX-4] Missing argument validation previously led to obscure
   * ArrayIndexOutOfBoundsException / silent no-ops.
   */
  private static void checkBounds(byte[] buf, int off, int len) {
    if (buf == null) {
      throw new NullPointerException("buffer");
    }
    if (off < 0 || len < 0 || len > buf.length - off) {
      throw new IndexOutOfBoundsException(
              "off=" + off + ", len=" + len + ", buffer.length=" + buf.length);
    }
  }

  /**
   * Fetches the next read-ahead window into the buffer, starting from the current read position.
   * The window never exceeds {@link #mBufferSize}, so a caller using a huge {@code read} buffer
   * cannot trigger an unbounded S3 fetch / allocation.
   * [FIX-2] Previously the fetch length was max(bufferSize, need) where need was the caller's
   * requested length, allowing huge multi-hundred-MB requests.
   */
  private synchronized void fillBuffer() throws IOException {
    long bufStart = mPos;
    long remaining = mContentLength - bufStart;
    if (remaining <= 0) {
      mBufferOffset = bufStart;
      mBufferLen = 0;
      return;
    }
    int fetchLen = (int) Math.min(remaining, (long) mBufferSize);
    try {
      ResponseBytes<?> response =
              mClient.getObjectAsBytes(GetObjectRequest.builder()
                      .bucket(mBucket)
                      .key(mKey)
                      .range(String.format("bytes=%d-%d", bufStart, bufStart + fetchLen - 1))
                      .build());
      byte[] data = response.asByteArray();
      // the response may be shorter than requested only at the end of the object
      int len = Math.min(data.length, fetchLen);
      if (len <= 0) {
        // [FIX-6] Object truncated/vanished while reading; treat as EOF but make it visible.
        LOG.warn("Empty range from S3 for key {} at offset {} (expected {})", mKey, bufStart,
                fetchLen);
        mBufferOffset = bufStart;
        mBufferLen = 0;
        return;
      }
      mBuffer = data;
      mBufferOffset = bufStart;
      mBufferLen = len;
    } catch (S3Exception e) {
      if (e.statusCode() == 404 || e.statusCode() == 416) {
        // [FIX-6] Treat both as EOF, but log so silent data loss is diagnosable.
        LOG.warn("Object {} returned status {} at offset {}; treating as EOF", mKey,
                e.statusCode(), bufStart);
        mBufferOffset = bufStart;
        mBufferLen = 0;
        return;
      }
      throw new IOException("Failed to read range from S3 for key " + mKey, e);
    }
  }

  @Override
  public synchronized int read() throws IOException {
    byte[] oneByte = new byte[1];
    int res = read(oneByte, 0, 1);
    if (res <= 0) {
      return -1;
    }
    return oneByte[0] & 0xff;
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    if (mClosed) {
      throw new IOException("stream already closed");
    }
    // [FIX-4] validate arguments
    checkBounds(buf, off, len);
    if (len == 0) {
      return 0;
    }
    if (mPos >= mContentLength) {
      // [FIX-1] Do NOT close() on EOF; just signal EOF. Repeated reads and seeks past EOF
      // are legal InputStream operations.
      return -1;
    }

    int totalRead = 0;
    while (totalRead < len) {
      if (mPos >= mContentLength) {
        break;
      }
      // Refill only when the buffer does not cover the current position.
      if (mPos < mBufferOffset || mPos >= mBufferOffset + mBufferLen) {
        fillBuffer();
        if (mBufferLen <= 0) {
          // EOF from our point of view (object truncated / concurrent delete).
          break;
        }
      }
      int bytesInBuffer = (int) (mBufferOffset + mBufferLen - mPos);
      if (bytesInBuffer <= 0) {
        break;
      }
      int toCopy = Math.min(len - totalRead, bytesInBuffer);
      System.arraycopy(mBuffer, (int) (mPos - mBufferOffset), buf, off + totalRead, toCopy);
      mPos += toCopy;
      totalRead += toCopy;
    }
    // [FIX-1] Return -1 only when nothing could be read; otherwise return short read.
    return totalRead > 0 ? totalRead : -1;
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (mClosed) {
      throw new IOException("stream already closed");
    }
    if (targetPos > mContentLength) {
      throw new IOException("Cannot seek after EOF, target: " + targetPos
              + " content length:" + mContentLength);
    }
    if (targetPos < 0) {
      throw new IOException("Cannot seek to negative offset");
    }
    mPos = targetPos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    // The data is always at the same position, no need for a new source.
    return false;
  }

  /**
   * Positioned read. Deliberately does not touch the sequential buffer or {@code mPos};
   * performs bounded-size ranged reads so a large {@code length} cannot OOM.
   * [FIX-3] Previously this fetched the entire requested range in one S3 call.
   */
  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if (mClosed) {
      throw new IOException("stream already closed");
    }
    // [FIX-4] validate arguments
    checkBounds(buffer, offset, length);
    if (length == 0) {
      return 0;
    }
    if (position < 0) {
      throw new IOException("Invalid position: " + position);
    }
    if (position >= mContentLength) {
      return -1;
    }

    int totalRead = 0;
    while (totalRead < length) {
      long cur = position + totalRead;
      if (cur >= mContentLength) {
        break;
      }
      // Cap each ranged GET at mBufferSize to bound memory usage.
      int chunk = (int) Math.min(
              Math.min((long) (length - totalRead), (long) mBufferSize),
              mContentLength - cur);
      long rangeEnd = cur + chunk - 1;
      try {
        byte[] data =
                mClient.getObjectAsBytes(GetObjectRequest.builder()
                                .bucket(mBucket)
                                .key(mKey)
                                .range(String.format("bytes=%d-%d", cur, rangeEnd))
                                .build())
                        .asByteArray();
        int toCopy = Math.min(chunk, data.length);
        if (toCopy <= 0) {
          LOG.warn("Empty positioned range from S3 for key {} at {}..{}", mKey, cur, rangeEnd);
          break;
        }
        System.arraycopy(data, 0, buffer, offset + totalRead, toCopy);
        totalRead += toCopy;
        if (toCopy < chunk) {
          // Short response only happens at end of object.
          break;
        }
      } catch (S3Exception e) {
        // [FIX-6] Consistent EOF handling with fillBuffer().
        if (e.statusCode() == 404 || e.statusCode() == 416) {
          LOG.warn("Object {} returned status {} for range {}-{}; treating as EOF", mKey,
                  e.statusCode(), cur, rangeEnd);
          break;
        }
        throw new IOException("Failed to read positioned range from S3 for key " + mKey, e);
      }
    }
    return totalRead > 0 ? totalRead : -1;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    if (mClosed) {
      throw new IOException("stream already closed");
    }
    // [FIX-4] validate arguments (also fixes the silent no-op on negative length)
    checkBounds(buffer, offset, length);
    if (length == 0) {
      return;
    }
    if (position < 0) {
      throw new IOException("Invalid position: " + position);
    }
    // [FIX-4] overflow-safe end-of-file check
    if (position > mContentLength || length > mContentLength - position) {
      throw new IOException("Read past end of file: position=" + position
              + ", length=" + length + ", content length=" + mContentLength);
    }

    int totalRead = 0;
    while (totalRead < length) {
      int nbytes = read(position + totalRead, buffer, offset + totalRead, length - totalRead);
      // [FIX-7] Defend against a zero-length read causing an infinite loop.
      if (nbytes <= 0) {
        throw new IOException("Read past end of file for key " + mKey);
      }
      totalRead += nbytes;
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  @Override
  public synchronized long getPos() throws IOException {
    return mPos;
  }

  @Override
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }
    mBufferLen = 0;
    mBuffer = new byte[0];
    mClosed = true;
    LOG.debug("S3InputStream closed for key: {}", mKey);
  }
}