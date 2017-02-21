package org.apache.storm.state;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.UnsignedBytes;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;

/**
 * Base implementation of iterator over {@link KeyValueState} which is based on binary type.
 */
public abstract class BaseBinaryStateIterator<K, V> implements Iterator<Map.Entry<K, V>> {

  private final PeekingIterator<Map.Entry<byte[], byte[]>> pendingPrepareIterator;
  private final PeekingIterator<Map.Entry<byte[], byte[]>> pendingCommitIterator;
  private final Set<byte[]> providedKeys;

  private boolean firstLoad = true;
  private PeekingIterator<Map.Entry<byte[], byte[]>> pendingIterator;
  private PeekingIterator<Map.Entry<byte[], byte[]>> cachedResultIterator;

  /**
   * Constructor.
   *
   * @param pendingPrepareIterator The iterator of pendingPrepare
   * @param pendingCommitIterator The iterator of pendingCommit
   */
  public BaseBinaryStateIterator(Iterator<Map.Entry<byte[], byte[]>> pendingPrepareIterator,
      Iterator<Map.Entry<byte[], byte[]>> pendingCommitIterator) {
    this.pendingPrepareIterator = Iterators.peekingIterator(pendingPrepareIterator);
    this.pendingCommitIterator = Iterators.peekingIterator(pendingCommitIterator);
    this.providedKeys = new TreeSet<>(UnsignedBytes.lexicographicalComparator());
  }

  @Override
  public boolean hasNext() {
    if (seekToAvailableEntry(pendingPrepareIterator)) {
      pendingIterator = pendingPrepareIterator;
      return true;
    }

    if (seekToAvailableEntry(pendingCommitIterator)) {
      pendingIterator = pendingCommitIterator;
      return true;
    }


    if (firstLoad) {
      // load the first part of entries
      fillCachedResultIterator();
      firstLoad = false;
    }

    while (true) {
      if (seekToAvailableEntry(cachedResultIterator)) {
        pendingIterator = cachedResultIterator;
        return true;
      }

      if (isEndOfDataFromStorage()) {
        break;
      }

      fillCachedResultIterator();
    }

    pendingIterator = null;
    return false;
  }

  private void fillCachedResultIterator() {
    Iterator<Map.Entry<byte[], byte[]>> iterator = loadChunkFromStateStorage();
    if (iterator != null) {
      cachedResultIterator = Iterators.peekingIterator(iterator);
    } else {
      cachedResultIterator = null;
    }
  }

  @Override
  public Map.Entry<K, V> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    Map.Entry<byte[], byte[]> keyValue = pendingIterator.next();

    K key = decodeKey(keyValue.getKey());
    V value = decodeValue(keyValue.getValue());

    providedKeys.add(keyValue.getKey());
    return new AbstractMap.SimpleEntry(key, value);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Load some part of state KVs from storage and returns iterator of cached data from storage.
   *
   * @return Iterator of loaded state KVs
   */
  protected abstract Iterator<Map.Entry<byte[],byte[]>> loadChunkFromStateStorage();

  /**
   * Check whether end of data is reached from storage state KVs.
   *
   * @return whether end of data is reached from storage state KVs
   */
  protected abstract boolean isEndOfDataFromStorage();

  /**
   * Decode key to convert byte array to state key type.
   *
   * @param key byte array encoded key
   * @return Decoded value of key
   */
  protected abstract K decodeKey(byte[] key);

  /**
   * Decode value to convert byte array to state value type.
   *
   * @param value byte array encoded value
   * @return Decoded value of value
   */
  protected abstract V decodeValue(byte[] value);

  /**
   * Get tombstone (deletion mark) value.
   *
   * @return tombstone (deletion mark) value
   */
  protected abstract byte[] getTombstoneValue();

  private boolean seekToAvailableEntry(PeekingIterator<Map.Entry<byte[], byte[]>> iterator) {
    if (iterator != null) {
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.peek();
        if (!providedKeys.contains(entry.getKey())) {
          if (Arrays.equals(entry.getValue(), getTombstoneValue())) {
            providedKeys.add(entry.getKey());
          } else {
            return true;
          }
        }

        iterator.next();
      }
    }

    return false;
  }

}
