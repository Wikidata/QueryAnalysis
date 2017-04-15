package accessfrequencymap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * 
 */

/**
 * @author adrian
 * @param <K>
 * @param <V>
 *
 */
public class AccessFrequencyMap<K, V> implements Map<K, V>
{

  /**
   * The list containing the mapped data.
   */
  List<AccessFrequencyMapEntry<K, V>> list = new ArrayList<AccessFrequencyMapEntry<K, V>>();

  @Override
  public void clear()
  {
    list = new LinkedList<AccessFrequencyMapEntry<K, V>>();
  }

  @Override
  public boolean containsKey(Object key)
  {
    for (AccessFrequencyMapEntry<K, V> entry : list) {
      if (entry.getKey().equals(key)) return true;
    }
    return false;
  }

  @Override
  public boolean containsValue(Object value)
  {
    for (AccessFrequencyMapEntry<K, V> entry : list) {
      if (entry.getValue().equals(value)) return true;
    }
    return false;
  }

  @Override
  public Set<Entry<K, V>> entrySet()
  {
    Set<Entry<K, V>> set = new TreeSet<Entry<K, V>>();
    for (AccessFrequencyMapEntry<K, V> entry : list) {
      set.add(new AccessFrequencyMapEntry<K, V>(entry.getKey(), entry.getValue()));
    }
    return set;
  }

  @Override
  public V get(Object key)
  {
    for (int i = 0; i < list.size(); i++) {
      AccessFrequencyMapEntry<K, V> entry = list.get(i);
      if (entry.getKey().equals(key)) {
        V value = entry.getValue();

        sortFrom(i);
        return value;
      }
    }
    return null;
  }

  @Override
  public boolean isEmpty()
  {
    return list.isEmpty();
  }

  @Override
  public Set<K> keySet()
  {
    // This is obviously stupid, but since the .hashCode of TupleExpr is broken (and it is not Comparable) we could not use a HashSet either.
    Set<K> set = new TreeSet<K>(new Comparator<K>() {
      @Override
      public int compare(K k1, K k2)
      {
        return k1.toString().compareTo(k2.toString());
      }
    });
    for (AccessFrequencyMapEntry<K, V> entry : list) {
      set.add(entry.getKey());
    }
    return set;
  }

  @Override
  public V put(K key, V value)
  {
    for (int i = 0; i < list.size(); i++) {
      AccessFrequencyMapEntry<K, V> entry = list.get(i);
      if (entry.getKey().equals(key)) {

        V oldValue = entry.getValue();
        entry.setValue(value);

        sortFrom(i);

        return oldValue;
      }
    }
    list.add(new AccessFrequencyMapEntry<K, V>(key, value));
    return null;
  }

  @Override
  public void putAll(Map< ? extends K, ? extends V> m)
  {
    for (Entry< ? extends K, ? extends V> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
    return;
  }

  @Override
  public V remove(Object key)
  {
    for (AccessFrequencyMapEntry<K, V> entry : list) {
      if (entry.getKey().equals(key)) {
        list.remove(entry);
      }
    }
    return null;
  }

  @Override
  public int size()
  {
    return list.size();
  }

  @Override
  public Collection<V> values()
  {
    List<V> valuesList = new LinkedList<V>();
    for (AccessFrequencyMapEntry<K, V> entry : list) {
      valuesList.add(entry.getValue());
    }
    return valuesList;
  }

  /**
   * @param i The position in the list that was changed and whose position should be checked
   */
  private void sortFrom(int i)
  {
    for (int j = i - 1; j >= 0; j--) {
      if (list.get(j).compareTo(list.get(i)) >= 0) {
        Collections.swap(list, j + 1, i);
        return;
      }
    }
    Collections.swap(list, 0, i);
    return;
  }
}
