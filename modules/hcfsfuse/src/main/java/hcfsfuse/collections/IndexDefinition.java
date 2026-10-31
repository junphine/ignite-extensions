package hcfsfuse.collections;

/**
 * Definition of an index in {@link IndexedSet}. This serves as the criterion for object lookup
 * within the HashSet.
 * <p>
 * Replacement of {@code alluxio.collections.IndexDefinition}.
 *
 * @param <T> class type of the object
 * @param <V> class type of the index field
 */
public abstract class IndexDefinition<T, V> {
  private final boolean mUnique;
  private final Type mType;

  /**
   * Constructs an IndexDefinition.
   *
   * @param unique true if this is an unique index
   */
  public IndexDefinition(boolean unique) {
    this(unique, Type.UNORDERED);
  }

  /**
   * Constructs an IndexDefinition.
   *
   * @param unique true if this is an unique index
   * @param type type of the index
   */
  public IndexDefinition(boolean unique, Type type) {
    mUnique = unique;
    mType = type;
  }

  /**
   * @return true if this is a unique index.
   */
  public boolean isUnique() {
    return mUnique;
  }

  /**
   * @return index type
   */
  public Type getType() {
    return mType;
  }

  /**
   * Gets the field value of an object under this index definition.
   *
   * @param o the object to get the field value from
   * @return the field value
   */
  public abstract V getFieldValue(T o);

  /**
   * Type of the index.
   */
  public enum Type {
    UNORDERED,
  }
}
