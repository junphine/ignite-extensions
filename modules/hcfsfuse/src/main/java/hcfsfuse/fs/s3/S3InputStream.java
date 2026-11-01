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
  private boolean mClosed = false;

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
   * Fetches the requested range into the buffer, starting from the current read position.
   */
  private synchronized void fillBuffer(int need) throws IOException {
    long bufStart = mPos;
    long remaining = mContentLength - bufStart;
    if (remaining <= 0) {
      return;
    }
    int fetchLen = (int) Math.min(remaining, Math.max(mBufferSize, need));
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
        throw new IOException("unexpected end of stream from S3 for key " + mKey);
      }
      mBuffer = data;
      mBufferOffset = bufStart;
      mBufferLen = len;
    } catch (S3Exception e) {
      if (e.statusCode() == 404 || e.statusCode() == 416) {
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
    if (len == 0) {
      return 0;
    }
    if (mPos >= mContentLength) {
      close();
      return -1;
    }
    int requested = (int) Math.min(len, mContentLength - mPos);
    // Fill the buffer if it does not cover the requested range.
    if (mPos < mBufferOffset || mPos + requested > mBufferOffset + mBufferLen) {
      fillBuffer(requested);
      if (mPos < mBufferOffset || mPos >= mBufferOffset + mBufferLen) {
        // no data available (e.g., concurrent truncation): EOF from our point of view
        close();
        return -1;
      }
    }
    int bytesInBuffer = (int) Math.max(0, mBufferOffset + mBufferLen - mPos);
    int bytesToCopy = Math.min(requested, bytesInBuffer);
    System.arraycopy(mBuffer, (int) (mPos - mBufferOffset), buf, off, bytesToCopy);
    mPos += bytesToCopy;
    return bytesToCopy;
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

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    if (mClosed) {
      throw new IOException("stream already closed");
    }
    if (length == 0) {
      return 0;
    }
    if (position < 0) {
      throw new IOException("Invalid position: " + position);
    }
    if (position >= mContentLength) {
      return -1;
    }
    long rangeEnd = Math.max(position, Math.min(position + length - 1, mContentLength - 1));
    try {
      byte[] data =
          mClient.getObjectAsBytes(GetObjectRequest.builder()
              .bucket(mBucket)
              .key(mKey)
              .range(String.format("bytes=%d-%d", position, rangeEnd))
              .build())
              .asByteArray();
      int toCopy = (int) Math.min(length, data.length);
      if (toCopy <= 0) {
        return -1;
      }
      System.arraycopy(data, 0, buffer, offset, toCopy);
      return toCopy;
    } catch (S3Exception e) {
      throw new IOException("Failed to read positioned range from S3 for key " + mKey, e);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    int nvOffset = offset;
    long nPosition = position;
    int nlength = length;
    int totalRead = 0;
    if (length == 0) {
      return;
    }
    if (position + length > mContentLength) {
      throw new IOException("Read past end of file: " + (position + length)
          + " content length: " + mContentLength);
    }
    while (totalRead < nlength) {
      int nbytes = read(nPosition + totalRead, buffer, nvOffset + totalRead, nlength - totalRead);
      if (nbytes < 0) {
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
