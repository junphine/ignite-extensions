package hcfsfuse.fs.s3;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link OutputStream} for writing to S3 (or any S3-compatible object store) with AWS SDK v2.
 * <p>
 * Data is accumulated in memory and uploaded:
 * <ul>
 * <li>for small files (below the multipart threshold), a single {@code PutObject} on close();
 * <li>for large files, parts are uploaded synchronously as the buffer fills up and the upload is
 * completed/aborted on close() so that memory usage stays bounded by one part.
 * </ul>
 * <p>
 * Note that S3 does not support append: the whole object is (re-)written from scratch.
 */
public final class S3OutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3OutputStream.class);

  /** S3 requires every multipart part except the last one to be at least 5MB. */
  static final long MIN_PART_SIZE = 5 * 1024 * 1024L;
  /** Default part size: 8MB (chosen to keep the memory footprint reasonable for FUSE access). */
  static final long DEFAULT_PART_SIZE = 8 * 1024 * 1024L;

  private final S3Client mClient;
  private final String mBucket;
  private final String mKey;
  private final FileSystem.Statistics mStatistics;
  private final int mPartSize;

  private final ByteArrayOutputStream mBuffer = new ByteArrayOutputStream();
  private final List<CompletedPart> mParts = new ArrayList<>();
  private long mWritten = 0;
  private String mUploadId = null;
  private int mPartNumber = 1;
  private boolean mClosed = false;

  /**
   * Creates a new instance of {@link S3OutputStream}.
   *
   * @param client the S3 client
   * @param bucket the bucket name
   * @param key the object key
   * @param statistics optional filesystem statistics to update (may be null)
   * @param partSize multipart part size in bytes; values below 5MB are clamped to 5MB
   */
  public S3OutputStream(S3Client client, String bucket, String key,
      FileSystem.Statistics statistics, long partSize) {
    mClient = client;
    mBucket = bucket;
    mKey = key;
    mStatistics = statistics;
    mPartSize = (int) Math.min(Math.max(partSize, MIN_PART_SIZE), Integer.MAX_VALUE);
  }

  /**
   * Creates a new instance of {@link S3OutputStream} with the default part size.
   */
  public S3OutputStream(S3Client client, String bucket, String key,
      FileSystem.Statistics statistics) {
    this(client, bucket, key, statistics, DEFAULT_PART_SIZE);
  }

  private void ensureOpen() throws IOException {
    if (mClosed) {
      throw new IOException("S3 stream already closed for key " + mKey);
    }
  }

  @Override
  public synchronized void write(int b) throws IOException {
    ensureOpen();
    mBuffer.write(b);
    mWritten++;
    drainIfFull();
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    ensureOpen();
    mBuffer.write(b, off, len);
    mWritten += len;
    if (mStatistics != null) {
      mStatistics.incrementBytesWritten(len);
    }
    drainIfFull();
  }

  /**
   * Uploads the buffered data as one part once the buffer is full.
   */
  private void drainIfFull() throws IOException {
    if (mBuffer.size() >= mPartSize) {
      uploadPart();
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    // nothing to do, flushing is deferred until close()
  }

  @Override
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    try {
      if (mUploadId == null) {
        // multipart never started: single put (possibly empty object).
        byte[] b = mBuffer.toByteArray();
        PutObjectRequest request = PutObjectRequest.builder()
            .bucket(mBucket)
            .key(mKey)
            .contentLength((long) b.length)
            .build();
        mClient.putObject(request, RequestBody.fromBytes(b));
      } else {
        if (mBuffer.size() > 0) {
          uploadPart();
        }
        mClient.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
            .bucket(mBucket)
            .key(mKey)
            .uploadId(mUploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(mParts).build())
            .build());
      }
    } catch (Exception e) {
      if (mUploadId != null) {
        try {
          mClient.abortMultipartUpload(AbortMultipartUploadRequest.builder()
              .bucket(mBucket).key(mKey).uploadId(mUploadId).build());
        } catch (Exception e2) {
          LOG.warn("Failed to abort multipart upload {} for key {}", mUploadId, mKey, e2);
        }
      }
      throw new IOException("Failed to close S3 stream for key " + mKey, e);
    }
    LOG.debug("S3OutputStream closed for key: {} ({} bytes)", mKey, mWritten);
  }

  /**
   * Uploads the buffered data as one part, starting a multipart upload if not yet started.
   */
  private synchronized void uploadPart() throws IOException {
    byte[] data = mBuffer.toByteArray();
    mBuffer.reset();
    if (data.length == 0) {
      return;
    }
    try {
      if (mUploadId == null) {
        CreateMultipartUploadResponse init = mClient.createMultipartUpload(
            CreateMultipartUploadRequest.builder().bucket(mBucket).key(mKey).build());
        mUploadId = init.uploadId();
      }
      UploadPartRequest req = UploadPartRequest.builder()
          .bucket(mBucket)
          .key(mKey)
          .uploadId(mUploadId)
          .partNumber(mPartNumber)
          .contentLength((long) data.length)
          .build();
      UploadPartResponse resp = mClient.uploadPart(req, RequestBody.fromBytes(data));
      mParts.add(CompletedPart.builder().eTag(resp.eTag()).partNumber(mPartNumber).build());
      mPartNumber++;
    } catch (Exception e) {
      throw new IOException("Failed to upload part for key " + mKey, e);
    }
  }
}
