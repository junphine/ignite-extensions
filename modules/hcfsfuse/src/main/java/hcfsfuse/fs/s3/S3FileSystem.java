package hcfsfuse.fs.s3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * A Hadoop {@link FileSystem} backed by S3 (or any S3-compatible object store), implemented
 * directly on top of AWS SDK v2 ({@code software.amazon.awssdk:s3}).
 * <p>
 * The filesystem is registered for the {@code s3} scheme through
 * {@code META-INF/services/org.apache.hadoop.fs.FileSystem}, so a mount can target an
 * S3 object store by running this FUSE mount with e.g.
 * {@code -r s3://mybucket/myprefix}. The filesystem may also be selected explicitly with
 * the hadoop config {@code fs.s3.impl} (or {@code fs.<any-scheme>.impl} together with a
 * matching URL).
 * <p>
 * Supported configuration keys (all on the hadoop Configuration passed to this fs):
 * <ul>
 * <li>{@code hcfs.s3.endpoint} optional endpoint (e.g. {@code http://host:9000}) for
 * S3-compatible stores (MinIO etc.); when set, path-style access is enabled by default</li>
 * <li>{@code hcfs.s3.region} signature region, defaults to {@code us-east-1}</li>
 * <li>{@code hcfs.s3.access.key} / {@code hcfs.s3.secret.key} / {@code hcfs.s3.session.token}
 * static credentials; when absent the SDK default credentials chain applies</li>
 * <li>{@code hcfs.s3.path.style.access} force path-style addressing (default true when an
 * endpoint is set, false otherwise)</li>
 * <li>{@code hcfs.s3.multipart.part.size} S3 multipart part size in bytes (default 8MB)</li>
 * <li>{@code hcfs.s3.stream.buffer.size} S3 read-ahead buffer size in bytes (default 1MB)</li>
 * <li>{@code hcfs.s3.owner} / {@code hcfs.s3.group} owner/group shown in file listings</li>
 * <li>{@code hcfs.s3.blocksize} default block size reported for files (default 128MB)</li>
 * </ul>
 */
public class S3FileSystem extends FileSystem {
  public static final Logger LOG = LoggerFactory.getLogger(S3FileSystem.class);

  /** The URI scheme this FileSystem is registered for. */
  public static final String SCHEME = "s3";

  public static final String S3_ENDPOINT_KEY = "hcfs.s3.endpoint";
  public static final String S3_REGION_KEY = "hcfs.s3.region";
  public static final String S3_ACCESS_KEY = "hcfs.s3.access.key";
  public static final String S3_SECRET_KEY = "hcfs.s3.secret.key";
  public static final String S3_SESSION_TOKEN = "hcfs.s3.session.token";
  public static final String S3_PATH_STYLE_ACCESS = "hcfs.s3.path.style.access";
  public static final String S3_MULTIPART_PART_SIZE_KEY = "hcfs.s3.multipart.part.size";
  public static final String S3_STREAM_BUFFER_SIZE_KEY = "hcfs.s3.stream.buffer.size";
  public static final String S3_OWNER_KEY = "hcfs.s3.owner";
  public static final String S3_GROUP_KEY = "hcfs.s3.group";
  public static final String S3_BLOCKSIZE_KEY = "hcfs.s3.blocksize";

  public static final long S3_DEFAULT_MULTIPART_PART_SIZE = 8 * 1024 * 1024L;
  public static final int S3_DEFAULT_STREAM_BUFFER_SIZE = 1024 * 1024;
  public static final long S3_DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024L;
  public static final int S3_MAX_BATCH_DELETE_OBJECTS = 1000;

  private S3Client mClient;
  private URI mUri;
  private Path mWorkingDir;
  private String mBucket;
  /** Directory prefix inside the bucket, with trailing '/' or empty for the bucket root. */
  private String mPrefix;
  private String mOwner;
  private String mGroup;
  private long mBlockSize;


  /**
   * This constructor is used by the hadoop service-loader machinery to detect the scheme
   * supported by this FileSystem (see {@link #getScheme()}).
   */
  public S3FileSystem() {
    LOG.debug("Creating S3FileSystem for scheme {}", SCHEME);
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  public URI getUri() {
    return mUri;
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setConf(conf);
    if (name != null && name.getScheme() != null && !SCHEME.equals(name.getScheme())) {
      throw new IOException(String.format("This filesystem only supports '%s://' URIs, got %s",
          SCHEME, name));
    }
    mUri = URI.create(String.format("%s://%s%s", SCHEME,
        name != null && name.getHost() != null ? name.getHost() : "",
        name != null && name.getPath() != null ? name.getPath() : ""));
    mBucket = name != null ? name.getHost() : "";
    String rawPath = name != null ? name.getPath() : "";
    if (rawPath != null && rawPath.length() > 1) {
      mPrefix = rawPath;
    } else {
      mPrefix = "";
    }
    if (mPrefix.length() > 0 && !mPrefix.endsWith("/")) {
      mPrefix = mPrefix + "/";
    }
    mOwner = conf.get(S3_OWNER_KEY, System.getProperty("user.name"));
    mGroup = conf.get(S3_GROUP_KEY, conf.get(S3_OWNER_KEY, System.getProperty("user.name")));
    mWorkingDir = new Path(name);
    mBlockSize = conf.getLong(S3_BLOCKSIZE_KEY, S3_DEFAULT_BLOCK_SIZE);
    mClient = buildClient(conf, name);
    LOG.debug("S3FileSystem initialized for bucket: {}, prefix: {}", mBucket, mPrefix);
  }

  /**
   * Creates the S3 client from the filesystem configuration.
   */
  private S3Client buildClient(Configuration conf, URI name) throws IOException {
    String accessKey = conf.get(S3_ACCESS_KEY);
    String secretKey = conf.get(S3_SECRET_KEY);
    String sessionToken = conf.get(S3_SESSION_TOKEN);
    String endpoint = conf.get(S3_ENDPOINT_KEY);
    S3ClientBuilder builder = S3Client.builder()
        .region(Region.of(conf.get(S3_REGION_KEY, "us-east-1")));

    if (accessKey != null && secretKey != null) {
      if (sessionToken != null) {
        builder.credentialsProvider(StaticCredentialsProvider.create(
            AwsSessionCredentials.create(accessKey, secretKey, sessionToken)));
      } else {
        builder.credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKey, secretKey)));
      }
    } else {
      builder.credentialsProvider(DefaultCredentialsProvider.create());
    }
    if (endpoint != null) {
      builder.endpointOverride(URI.create(endpoint));
      builder.serviceConfiguration(S3Configuration.builder()
          .pathStyleAccessEnabled(conf.getBoolean(S3_PATH_STYLE_ACCESS, true)).build());
    } else if (conf.getBoolean(S3_PATH_STYLE_ACCESS, false)) {
      builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
    }
    return builder.build();
  }

  @Override
  public void close() throws IOException {
    if (mClient != null) {
      try {
        mClient.close();
      } finally {
        mClient = null;
      }
    }
    super.close();
  }

  /**
   * Translates a path into an S3 object key, relative to the bucket root (never empty).
   */
  private String pathToKey(final Path path) throws IOException {
    Path p = path;
    if (!path.isAbsolute()) {
      p = new Path(mWorkingDir, path);
    }
    URI pathUri = p.toUri();
    String pathScheme = pathUri.getScheme() == null ? "" : pathUri.getScheme();
    if (!pathScheme.isEmpty() && !pathScheme.equals(SCHEME)) {
      throw new IOException("Path outside of the " + SCHEME + " filesystem: " + path);
    }
    String pathBucket = pathUri.getHost();
    if (pathBucket != null && !pathBucket.isEmpty() && !pathBucket.equals(mBucket)) {
      throw new IOException("Path on a different bucket than the filesystem's: " + path);
    }
    String key = pathUri.getPath();
    if (key.startsWith("/")) {
      key = key.substring(1);
    }
    return key;
  }

  /**
   * Translates an S3 object key into a path in this filesystem.
   */
  private Path keyToPath(final String key) {
    return new Path(getUri().getScheme(),
        getUri().getAuthority(),
        new StringBuilder("/").append(key).toString());
  }

  private FileStatus newFileStatus(long length, boolean isDir, long modificationTime, Path path) {
    return new FileStatus(length, isDir, 1, mBlockSize, modificationTime, modificationTime,
        isDir ? FsPermission.getDirDefault() : FsPermission.getFileDefault(),
        mOwner, mGroup, null, path);
  }

  @Override
  public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
    final Path path = f;
    final String key = pathToKey(path);
    if (key.isEmpty()) {
      throw new IOException("Can't open the bucket itself: " + path);
    }
    long size;
    try {
      HeadObjectResponse response = mClient.headObject(r -> r.bucket(mBucket).key(key));
      size = response.contentLength() != null ? response.contentLength() : 0L;
    } catch (NoSuchKeyException e) {
      throw new FileNotFoundException(path.toString());
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        throw new FileNotFoundException(path.toString());
      }
      throw new IOException("Failed to open " + path, e);
    }
    final int bufferSizeConfured =
        getConf().getInt(S3_STREAM_BUFFER_SIZE_KEY, S3_DEFAULT_STREAM_BUFFER_SIZE);
    if (statistics != null) {
      statistics.incrementReadOps(1);
    }
    return new FSDataInputStream(
        new S3InputStream(mClient, mBucket, key, size, bufferSizeConfured));
  }

  @Override
  public FSDataOutputStream create(final Path f, final FsPermission permission,
      final boolean overwrite, final int bufferSize, final short replication,
      final long blockSize, final Progressable progress) throws IOException {
    Path path = f;
    String key = pathToKey(path);
    if (key.isEmpty()) {
      throw new IOException("Can't create the bucket itself: " + path);
    }
    if (!overwrite && exists(path)) {
      throw new FileAlreadyExistsException(path.toString());
    }
    if (key.endsWith("/")) {
      throw new IOException("Cannot create object with directory-like key: " + path);
    }
    // If a directory marker exists at this exact key (the path was a directory before),
    // remove it so the plain object replaces the directory.
    try {
      mClient.headObject(r -> r.bucket(mBucket).key(key + "/"));
      mClient.deleteObject(DeleteObjectRequest.builder().bucket(mBucket).key(key + "/").build());
    } catch (NoSuchKeyException e) {
      // no marker, nothing to remove
    } catch (S3Exception e) {
      if (e.statusCode() != 404) {
        throw new IOException("Failed to create " + path, e);
      }
    }
    final long partSize =
        getConf().getLong(S3_MULTIPART_PART_SIZE_KEY, S3_DEFAULT_MULTIPART_PART_SIZE);
    S3OutputStream out = new S3OutputStream(mClient, mBucket, key, statistics, partSize);
    if (statistics != null) {
      statistics.incrementWriteOps(1);
    }
    return new FSDataOutputStream(out, statistics);
  }

  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize,
      final Progressable progress) throws IOException {
    throw new IOException("Append is not supported on the S3 file system");
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    final String srcKey = pathToKey(src);
    final String dstKey = pathToKey(dst);
    if (srcKey.isEmpty()) {
      LOG.error("Cannot rename the bucket root {}", src);
      return false;
    }
    if (srcKey.equals(dstKey)) {
      LOG.warn("rename {} to {}: source and destination are the same", src, dst);
      return true;
    }
    FileStatus srcStatus = getFileStatus(src);
    FileStatus dstStatus;
    try {
      dstStatus = getFileStatus(dst);
    } catch (FileNotFoundException e) {
      dstStatus = null;
    }
    Path renamedDst = dst;
    if (dstStatus != null && dstStatus.isFile()) {
      LOG.error("Cannot rename {} to {}: destination is a file", src, dst);
      return false;
    }
    if (dstStatus != null && dstStatus.isDirectory()) {
      // move into the destination directory
      renamedDst = new Path(dst, src.getName());
    }
    boolean result;
    if (srcStatus.isFile()) {
      result = copyFile(srcKey, pathToKey(renamedDst));
      if (result) {
        deleteObjectIfExists(srcKey);
      }
    } else {
      result = copyDirectory(src, renamedDst);
      if (result) {
        delete(new Path(src.toUri()), true);
      }
    }
    return result;
  }

  /**
   * Copies a single object, returning true when the copy succeeded.
   */
  private boolean copyFile(String srcKey, String dstKey) throws IOException {
    try {
      mClient.copyObject(CopyObjectRequest.builder()
          .sourceBucket(mBucket)
          .sourceKey(srcKey)
          .destinationBucket(mBucket)
          .destinationKey(dstKey)
          .build());
      return true;
    } catch (S3Exception e) {
      throw new IOException(String.format(
          "Failed to copy key %s to %s in bucket %s", srcKey, dstKey, mBucket), e);
    }
  }

  /**
   * Copies a directory and all of its recursively listed content.
   */
  private boolean copyDirectory(Path src, Path dst) throws IOException {
    String srcKey = pathToKey(src);
    String dstKey = pathToKey(dst);
    String srcPrefix = srcKey + "/";
    String dstPrefix = dstKey + "/";
    // create the destination directory marker
    mClient.putObject(r -> r.bucket(mBucket).key(dstPrefix), RequestBody.fromBytes(new byte[0]));
    ListObjectsV2Request request = ListObjectsV2Request.builder()
        .bucket(mBucket)
        .prefix(srcPrefix)
        .maxKeys(S3_MAX_BATCH_DELETE_OBJECTS)
        .build();
    ListObjectsV2Response response = mClient.listObjectsV2(request);
    while (response != null) {
      for (var obj : response.contents()) {
        String oldKey = obj.key();
        if (oldKey.equals(srcPrefix)) {
          continue;
        }
        String suffix = oldKey.substring(srcPrefix.length());
        copyFile(oldKey, dstPrefix + suffix);
      }
      if (response.isTruncated() != null && response.isTruncated()) {
        request = request.toBuilder().continuationToken(response.nextContinuationToken()).build();
        response = mClient.listObjectsV2(request);
      } else {
        break;
      }
    }
    return true;
  }

  private void deleteObjectIfExists(String key) throws IOException {
    try {
      mClient.deleteObject(DeleteObjectRequest.builder().bucket(mBucket).key(key).build());
    } catch (S3Exception e) {
      throw new IOException("Failed to delete key " + key, e);
    }
  }

  @Override
  public boolean delete(final Path f, final boolean recursive) throws IOException {
    String key = pathToKey(f);
    if (key.isEmpty()) {
      throw new IOException("Cannot delete the bucket root: " + f);
    }
    FileStatus status = getFileStatus(f);
    if (status.isFile()) {
      deleteObjectIfExists(key);
      // also remove a possible directory marker written before by mkdirs
      deleteObjectIfExists(key + "/");
      return true;
    }
    // directory
    String dirPrefix = key + "/";
    if (!recursive) {
      ListObjectsV2Response descendants = mClient.listObjectsV2(
          ListObjectsV2Request.builder().bucket(mBucket).prefix(dirPrefix)
              .maxKeys(2).build());
      boolean hasChildren = descendants.contents().stream().anyMatch(obj ->
              !obj.key().equals(dirPrefix))
          || !descendants.commonPrefixes().isEmpty();
      if (hasChildren) {
        throw new IOException("Directory " + f + " is not empty, recursive delete not allowed");
      }
    }
    // batch-delete every object under the directory, marker included
    ListObjectsV2Request request = ListObjectsV2Request.builder()
        .bucket(mBucket)
        .prefix(dirPrefix)
        .maxKeys(S3_MAX_BATCH_DELETE_OBJECTS)
        .build();
    ListObjectsV2Response response = mClient.listObjectsV2(request);
    while (response != null) {
      for (var obj : response.contents()) {
        mClient.deleteObject(DeleteObjectRequest.builder().bucket(mBucket).key(obj.key()).build());
      }
      if (response.isTruncated() != null && response.isTruncated()) {
        request = request.toBuilder().continuationToken(response.nextContinuationToken()).build();
        response = mClient.listObjectsV2(request);
      } else {
        break;
      }
    }
    return true;
  }

  @Override
  public FileStatus[] listStatus(final Path f) throws IOException {
    final String key = pathToKey(f);
    FileStatus dirStatus = getFileStatus(f);
    if (!dirStatus.isDirectory()) {
      throw new IOException("Not a directory: " + f);
    }
    String prefix = key.isEmpty() ? "" : key + "/";
    List<FileStatus> statuses = new ArrayList<>();
    ListObjectsV2Request request = ListObjectsV2Request.builder()
        .bucket(mBucket)
        .prefix(prefix)
        .delimiter("/")
        .maxKeys(S3_MAX_BATCH_DELETE_OBJECTS)
        .build();
    ListObjectsV2Response response = mClient.listObjectsV2(request);
    while (response != null) {
      for (var obj : response.contents()) {
        // skip the directory marker of the listed directory itself
        if (obj.key().equals(prefix)) {
          continue;
        }
        boolean isDir = obj.key().endsWith("/");
        Path objectPath = keyToPath(obj.key());
        long modificationTime = obj.lastModified() != null
            ? obj.lastModified().toEpochMilli()
            : 0L;
        long size = isDir || obj.size() == null ? 0 : obj.size();
        statuses.add(newFileStatus(size, isDir, modificationTime, objectPath));
      }
      for (var commonPrefix : response.commonPrefixes()) {
        Path objectPath = keyToPath(commonPrefix.prefix());
        statuses.add(newFileStatus(0, true, 0, objectPath));
      }
      if (response.isTruncated() != null && response.isTruncated()) {
        request = request.toBuilder().continuationToken(response.nextContinuationToken()).build();
        response = mClient.listObjectsV2(request);
      } else {
        break;
      }
    }
    if (statistics != null) {
      statistics.incrementReadOps(1);
    }
    return statuses.toArray(new FileStatus[0]);
  }

  /**
   * Checks whether the key exists, either as a plain object or as a directory (marker, or
   * object(s) living under the prefix).
   */
  private boolean keyExists(String key) throws IOException {
    if (key.isEmpty()) {
      return true;
    }
    try {
      mClient.headObject(r -> r.bucket(mBucket).key(key));
      return true;
    } catch (NoSuchKeyException e) {
      // fall through to the directory check
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        // fall through to the directory check
      } else {
        throw new IOException("Failed to check existence of key " + key, e);
      }
    }
    try {
      ListObjectsV2Response response = mClient.listObjectsV2(ListObjectsV2Request.builder()
          .bucket(mBucket)
          .prefix(key + "/")
          .maxKeys(1)
          .build());
      return !response.contents().isEmpty() || !response.commonPrefixes().isEmpty();
    } catch (S3Exception e) {
      throw new IOException("Failed to check existence of key " + key, e);
    }
  }

  @Override
  public FileStatus getFileStatus(final Path f) throws IOException {
    final String key = pathToKey(f);
    final Path path = f;
    if (key.isEmpty()) {
      // the bucket root is always a directory
      FileStatus bucketStatus = newFileStatus(0, true, 0, path);
      return bucketStatus;
    }
    try {
      HeadObjectResponse response = mClient.headObject(r -> r.bucket(mBucket).key(key));
      long modificationTime = response.lastModified() != null
          ? response.lastModified().toEpochMilli()
          : 0L;
      boolean isDir = key.endsWith("/");
      return newFileStatus(response.contentLength() != null ? response.contentLength() : 0L,
          isDir, modificationTime, path);
    } catch (NoSuchKeyException e) {
      // fall through to the directory check
    } catch (S3Exception e) {
      if (e.statusCode() == 404) {
        // fall through to the directory check
      } else {
        throw new IOException("Failed to get file status for " + path, e);
      }
    }
    if (keyExists(key)) {
      // exists as a directory (implicit or a marker object)
      FileStatus dirStatus = newFileStatus(0, true, 0, path);
      return dirStatus;
    }
    throw new FileNotFoundException(path.toString());
  }

  @Override
  public boolean mkdirs(final Path f, final FsPermission permission) throws IOException {
    String key = pathToKey(f);
    if (key.isEmpty()) {
      return true; // the bucket root always exists
    }
    try {
      FileStatus existing = getFileStatus(f);
      if (existing.isDirectory()) {
        return true; // already a directory
      }
      throw new FileAlreadyExistsException("Directory already exists as a file: " + f);
    } catch (FileNotFoundException e) {
      // fall through, create the marker
    }
    String markerKey = key + "/";
    try {
      mClient.putObject(r -> r.bucket(mBucket).key(markerKey),
          RequestBody.fromBytes(new byte[0]));
    } catch (S3Exception e) {
      throw new IOException("Failed to create directory " + f, e);
    }
    return true;
  }

  @Override
  public void setWorkingDirectory(final Path newDir) {
    mWorkingDir = newDir != null ? newDir : new Path(mUri);
  }

  @Override
  public Path getWorkingDirectory() {
    return mWorkingDir;
  }

  @Override
  public long getDefaultBlockSize() {
    return mBlockSize;
  }

  @Override
  public void setPermission(final Path p, final FsPermission permission) throws IOException {
    LOG.debug("setPermission on {} is a no-op (S3 has no permissions)", p);
  }

  @Override
  public void setOwner(final Path p, final String username, final String groupname)
      throws IOException {
    LOG.debug("setOwner on {} is a no-op (S3 has no ownership)", p);
  }

  @Override
  public void setTimes(final Path p, final long mtime, final long atime) throws IOException {
    LOG.debug("setTimes on {} is a no-op (S3 has no mutable timestamps)", p);
  }
}
