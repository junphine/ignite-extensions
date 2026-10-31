package hcfsfuse.fuse;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utility used to store the file information of an open file in the HCFS-FUSE process.
 * <p>
 * Replacement of {@code alluxio.fuse.OpenFileEntry}.
 *
 * @param <InT> input stream type
 * @param <OutT> output stream type
 */
public final class OpenFileEntry<InT extends InputStream, OutT extends OutputStream>
    implements Closeable {
  private final long mId;
  private String mPath;
  private final InT mIn;
  private final OutT mOut;
  private long mWriteOffset;

  /**
   * Creates a new instance of {@link OpenFileEntry}.
   *
   * @param id the file id
   * @param path the file path
   * @param in the input stream to read the file data, null if not open for read
   * @param out the output stream to write the file data, null if not open for write
   */
  public OpenFileEntry(long id, String path, InT in, OutT out) {
    mId = id;
    mPath = path;
    mIn = in;
    mOut = out;
  }

  /**
   * @return the file id
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the file path
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Sets the file path.
   *
   * @param path the path to set
   */
  public void setPath(String path) {
    mPath = path;
  }

  /**
   * @return the input stream
   */
  public InT getIn() {
    return mIn;
  }

  /**
   * @return the output stream
   */
  public OutT getOut() {
    return mOut;
  }

  /**
   * @return the write offset
   */
  public long getWriteOffset() {
    return mWriteOffset;
  }

  /**
   * Sets the write offset.
   *
   * @param writeOffset the offset to set
   */
  public void setWriteOffset(long writeOffset) {
    mWriteOffset = writeOffset;
  }

  @Override
  public void close() throws IOException {
    if (mIn != null) {
      mIn.close();
    }
    if (mOut != null) {
      mOut.close();
    }
  }
}
