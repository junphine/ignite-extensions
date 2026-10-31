package hcfsfuse.collections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.IdentityHashMap;
import java.util.function.Predicate;

/**
 * A thread-safe set of objects indexed by one or multiple fields. Each index keeps track of the
 * set of objects sharing a field value.
 * <p>
 * This is a lightweight, self-contained replacement of {@code alluxio.collections.IndexedSet},
 * providing only the operations used by HCFS-FUSE: {@link #add}, {@link #remove},
 * {@link #getFirstByField}, {@link #getByField}, {@link #size} and {@link #clear}.
 * <p>
 * Notes:
 * <ul>
 * <li>Unique indexes cannot be invalidated; it is an error to add an object whose unique field
 * already exists in this set. Adding such an object returns false and object state stays unmodified.
 * <li>Objects not both unique and not both non-unique may coexist.
 * </ul>
 *
 * @param <T> type of objects to keep
 */
public class IndexedSet<T> {
  public static final int DEFAULT_INDEX_LIST_SIZE = 3;

  /**
   * List of index definitions.
   */
  private final List<IndexDefinition<T, ?>> mIndexDefinitions;

  /**
   * Map of field value -> single object (for unique indexes) or list of objects (for
   * non-unique indexes), grouped by index definition.
   */
  private final Map<IndexDefinition<T, ?>, Map<Object, Object>> mFieldMaps;

  /**
   * Set of objects owned by this set (identity-based).
   */
  private final Set<T> mObjectsSet;

  /**
   * Constructs a new {@link IndexedSet} instance with specified indexes.
   *
   * @param indexDefinitions the list of index definitions
   */
  public IndexedSet(IndexDefinition<T, ?>... indexDefinitions) {
    List<IndexDefinition<T, ?>> indexDefList = new ArrayList<>(indexDefinitions.length);
    Collections.addAll(indexDefList, indexDefinitions);
    mIndexDefinitions = Collections.unmodifiableList(indexDefList);
    mFieldMaps = new HashMap<>(indexDefinitions.length * 4 / 3);
    for (IndexDefinition<T, ?> indexDefinition : indexDefList) {
      mFieldMaps.put(indexDefinition, new HashMap<>());
    }
    mObjectsSet = Collections.newSetFromMap(new IdentityHashMap<>());
  }

  /**
   * Constructs a new {@link IndexedSet} instance with defined indexes.
   *
   * @param indexDefinitions the list of index definitions
   */
  public IndexedSet(List<IndexDefinition<T, ?>> indexDefinitions) {
    mIndexDefinitions = Collections.unmodifiableList(new ArrayList<>(indexDefinitions));
    mFieldMaps = new HashMap<>(indexDefinitions.size() * 4 / 3);
    for (IndexDefinition<T, ?> indexDefinition : indexDefinitions) {
      mFieldMaps.put(indexDefinition, new HashMap<>());
    }
    mObjectsSet = Collections.newSetFromMap(new IdentityHashMap<>());
  }

  /**
   * Adds an object o to the IndexedSet.
   *
   * An object with a unique field that already exists in this set will be rejected and the field
   * state of this object will stay as if this method is never called.
   *
   * @param o object to add
   * @return false if o is rejected by the set due to a unique field collision, true otherwise
   * @throws IllegalArgumentException if any unique field of o is null
   */
  public boolean add(T o) {
    Objects.requireNonNull(o, "object is null");
    synchronized (mObjectsSet) {
      // pre-check unique field collision
      for (IndexDefinition<T, ?> indexDefinition : mIndexDefinitions) {
        if (indexDefinition.isUnique()) {
          Object o1 = getByFieldUniqueOrNull(indexDefinition, indexDefinition.getFieldValue(o));
          if (o1 != null) {
            return false;
          }
        }
      }
      // insert into every index
      for (IndexDefinition<T, ?> indexDefinition : mIndexDefinitions) {
        Object fieldValue = indexDefinition.getFieldValue(o);
        if (indexDefinition.isUnique()) {
          if (fieldValue == null) {
            throw new IllegalArgumentException(
                "Unique field value of " + o + " is null in index " + indexDefinition);
          }
          mFieldMaps.get(indexDefinition).put(fieldValue, o);
        } else {
          Map<Object, Object> fieldMap = mFieldMaps.get(indexDefinition);
          Object currentList = fieldMap.get(fieldValue);
          if (currentList == null) {
            List<T> newList = new ArrayList<>(DEFAULT_INDEX_LIST_SIZE);
            newList.add(o);
            fieldMap.put(fieldValue, newList);
          } else {
            @SuppressWarnings("unchecked")
            List<T> listOfObjects = (List<T>) currentList;
            listOfObjects.add(o);
          }
        }
      }
      mObjectsSet.add(o);
      return true;
    }
  }

  /**
   * Removes an object o from the IndexedSet.
   *
   * @param o the object to remove
   * @return false if o is not in the set, true otherwise
   */
  public boolean remove(T o) {
    synchronized (mObjectsSet) {
      boolean toRemove = mObjectsSet.remove(o);
      if (toRemove) {
        for (IndexDefinition<T, ?> indexDefinition : mIndexDefinitions) {
          Object fieldValue = indexDefinition.getFieldValue(o);
          Map<Object, Object> fieldMap = mFieldMaps.get(indexDefinition);
          if (indexDefinition.isUnique()) {
            fieldMap.remove(fieldValue);
          } else {
            @SuppressWarnings("unchecked")
            List<T> listOfObjects = (List<T>) fieldMap.get(fieldValue);
            if (listOfObjects != null) {
              listOfObjects.remove(o);
              if (listOfObjects.isEmpty()) {
                fieldMap.remove(fieldValue);
              }
            }
          }
        }
      }
      return toRemove;
    }
  }

  /**
   * @return number of objects in this set
   */
  public int size() {
    synchronized (mObjectsSet) {
      return mObjectsSet.size();
    }
  }

  /**
   * Empties the indexSet.
   */
  public void clear() {
    synchronized (mObjectsSet) {
      mObjectsSet.clear();
      for (IndexDefinition<T, ?> indexDefinition : mIndexDefinitions) {
        mFieldMaps.get(indexDefinition).clear();
      }
    }
  }

  /**
   * Gets one object with the specified field value.
   * An arbitrary object is picked among objects belonging to a non-unique index.
   *
   * @param indexDefinition the index to look up by
   * @param value the value of the index field
   * @return an arbitrary object with the specified field value
   * @throws NoSuchElementException if the index doesn't exist in this set
   */
  public T getFirstByField(IndexDefinition<T, ?> indexDefinition, Object value) {
    synchronized (mObjectsSet) {
      Object o = getByFieldInternal(indexDefinition, value);
      if (o instanceof List) {
        @SuppressWarnings("unchecked")
        List<T> listOfObjects = (List<T>) o;
        return listOfObjects.isEmpty() ? null : listOfObjects.get(0);
      }
      @SuppressWarnings("unchecked")
      T returnObj = (T) o;
      return returnObj;
    }
  }

  /**
   * @return a set of all objects that have the specified field value
   * @throws NoSuchElementException if the index doesn't exist in this set
   */
  public Set<T> getByField(IndexDefinition<T, ?> indexDefinition, Object value) {
    synchronized (mObjectsSet) {
      Set<T> results = new HashSet<>();
      Object o = getByFieldInternal(indexDefinition, value);
      if (o instanceof List) {
        @SuppressWarnings("unchecked")
        List<T> listOfObjects = (List<T>) o;
        results.addAll(listOfObjects);
      } else {
        @SuppressWarnings("unchecked")
        T returnObj = (T) o;
        if (returnObj != null) {
          results.add(returnObj);
        }
      }
      return results;
    }
  }

  /**
   * Removes and returns every object that the predicate returns true for.
   *
   * @param predicate the predicate to test objects with
   * @return a set of removed objects
   */
  public Set<T> removeByFieldIf(Predicate<T> predicate) {
    synchronized (mObjectsSet) {
      Set<T> toRemove = new HashSet<>();
      for (T o : mObjectsSet) {
        if (predicate.test(o)) {
          toRemove.add(o);
        }
      }
      for (T o : toRemove) {
        remove(o);
      }
      return toRemove;
    }
  }

  /**
   * Gets the object for a unique index, or null when not present.
   *
   * @throws NoSuchElementException if the index doesn't exist in this set
   */
  private Object getByFieldUniqueOrNull(IndexDefinition<T, ?> indexDefinition, Object value) {
    Map<Object, Object> fieldMap = getAndCheckFieldMap(indexDefinition);
    return fieldMap.get(value);
  }

  private Object getByFieldInternal(IndexDefinition<T, ?> indexDefinition, Object value) {
    Map<Object, Object> fieldMap = getAndCheckFieldMap(indexDefinition);
    return fieldMap.get(value);
  }

  private Map<Object, Object> getAndCheckFieldMap(IndexDefinition<T, ?> indexDefinition) {
    Map<Object, Object> fieldMap = mFieldMaps.get(indexDefinition);
    if (fieldMap == null) {
      throw new NoSuchElementException(
          "The index " + indexDefinition + " does not belong to this set");
    }
    return fieldMap;
  }
}
