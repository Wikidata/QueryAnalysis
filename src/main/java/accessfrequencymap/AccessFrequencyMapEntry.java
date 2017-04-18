package accessfrequencymap;

import java.util.Map.Entry;

/**
 * @param <K>
 * @param <V>
 * @author adrian
 */
public class AccessFrequencyMapEntry<K, V> implements Entry<K, V>, Comparable<AccessFrequencyMapEntry<K, V>>
{
  /**
   * The key to the entry.
   */
  private K key;

  /**
   * The value to the entry.
   */
  private V value;

  /**
   * How often this entry was accessed.
   */
  private Integer timesAccessed = 0;

  /**
   * @param key2   The key for this entry
   * @param value2 The value for this entry
   */
  public AccessFrequencyMapEntry(K key2, V value2)
  {
    this.key = key2;
    this.value = value2;
  }

  @Override
  public K getKey()
  {
    return key;
  }

  @Override
  public V getValue()
  {
    timesAccessed++;
    return value;
  }

  /**
   * @return {@link timesAccessed}
   */
  public Integer getTimesAccessed()
  {
    return timesAccessed;
  }

  @Override
  public V setValue(V arg0)
  {
    V oldValue = value;
    value = arg0;
    return oldValue;
  }

  @Override
  public int compareTo(AccessFrequencyMapEntry<K, V> arg0)
  {
    return timesAccessed.compareTo(arg0.getTimesAccessed());
  }
}
