# hcfsfuse

`hcfsfuse` is a fuse program which can access any hcfs implemented file system.
Such as local filesystem, hdfs, ozone, alluxio, oss, cos, s3, and so on.

# how to build

A fat jar will be generated in the target folder named hcfsfuse-<VERSION>-jar-with-dependencies.jar after build the project successfully.

A Simple way like following

```bash
$ mvn clean package
```

If you want to specify the version of alluxio, hadoop or ozone, reference the following example.

```bash
 mvn clean package -Dhadoop.version=3.4.1 -Dozone.version=1.0.0
```

# how to run

```bash
$ java -Dlog4j.configuration=file:<LOG4j_FILE_PATH> -jar target/hcfsfuse-1.0.0-SNAPSHOT-jar-with-dependencies.jar -c core-site.xml -c another-site.xml -m /Users/mbl/fusefs -r file:///tmp/
```
```bash
$ java -cp hadoop/* hcfsfuse.fuse.HCFSFuse -m /home/zhangjunfeng/igfs -r file:///mnt/d/Data//uploads/  -debug -o allow_other
```

Then, you can ls, touch, cp, rm, cat file or directory under `/Users/mbl/fusefs`.

If there are something wrong strangely, check if forgot unmount the mount point folder and unmount it manually as following.

```bash
umount /Users/mbl/fusefs
```

# how to mount S3 (or any S3-compatible store)

The module ships a Hadoop `FileSystem` implementation for the `s3://` scheme built directly
on AWS SDK v2 (`software.amazon.awssdk:s3`), registered via
`META-INF/services/org.apache.hadoop.fs.FileSystem`, so no extra `fs.s3.impl` config is needed.
Mount an S3 (or MinIO/其他兼容存储) prefix by passing its `s3://` URL as the `-r` target root:

```bash
$ java -jar target/hcfsfuse-<VERSION>-jar-with-dependencies.jar \
    -c s3-site.xml -m /mnt/s3fuse -r s3://mybucket/myprefix/
```

with the following keys enabled in the given configuration files (hadoop `Configuration`):

| key | description |
| --- | --- |
| `hcfs.s3.endpoint` | optional, endpoint of an S3-compatible store (e.g. `http://host:9000`). When set, path-style bucket addressing is enabled by default |
| `hcfs.s3.region` | signature region, default `us-east-1` |
| `hcfs.s3.access.key` / `hcfs.s3.secret.key` | optional static credentials; when unset the AWS SDK default credential chain applies (env vars, profile, IMDS, ...) |
| `hcfs.s3.session.token` | optional session token for temporary credentials |
| `hcfs.s3.path.style.access` | force path-style addressing, default `true` when an endpoint is set |
| `hcfs.s3.chunked.encoding` | SDK-side aws-chunked payload signing on `PutObject`/`UploadPart` (`x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`). Default `false`: some S3-compatible servers do not decode this framing and would store the raw `n;chunk-signature=...` frames in the object. Set `true` only for servers that properly implement streaming signatures |
| `hcfs.s3.multipart.part.size` | size in bytes of each multipart upload part, default 8MB |
| `hcfs.s3.stream.buffer.size` | read-ahead buffer size in bytes, default 1MB |
| `hcfs.s3.owner` / `hcfs.s3.group` | owner/group reported in file listings, defaults to the mounting user |

For example, for MinIO:

```xml
<configuration>
  <property><name>hcfs.s3.endpoint</name><value>http://127.0.0.1:9000</value></property>
  <property><name>hcfs.s3.access.key</name><value>minioadmin</value></property>
  <property><name>hcfs.s3.secret.key</name><value>minioadmin</value></property>
</configuration>
```

Notes:
- Files are downloaded with a read-ahead buffer, and uploaded through S3 multipart upload on
  close (S3 has no append/flush semantics, so data becomes visible after release).
- `rename`/`delete` on directories work on the whole prefix; `chmod`/`chown`/`setTimes` are
  no-ops since object stores have no such metadata.

# Notice

**There are a lot of reference from Alluxio fuse.**