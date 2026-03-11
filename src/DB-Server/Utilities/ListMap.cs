using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using MemoryPack;
using Microsoft.Extensions.Logging;

namespace Minerva.DB_Server;

[MemoryPackable]
public partial struct ListMap<K, V> : IDictionary<K, V> where K : IEquatable<K> 
{
    public List<K> keys;
    public List<V> values;

    private readonly ILogger _logger = LoggerManager.GetLogger();

    public ListMap()
    {
        keys = [];
        values = [];
    }

    public void Add(K key, V value)
    {
        for (int i = 0; i < keys.Count; i++)
        {
            if (keys[i].Equals(key))
            {
                throw new ArgumentException($"An item with the same key has already been added. Key: {key}");
            }
        }

        keys.Add(key);
        values.Add(value);

        if (keys.Count > 10)
        {
            _logger.LogWarning("ListMap contains more than 10 items ({Count} items). Consider using a different data structure for better performance.", keys.Count);
        }

    }

    public bool ContainsKey(K key)
    {
        for (int i = 0; i < keys.Count; i++)
        {
            if (keys[i].Equals(key))
                return true;
        }
        return false;
    }

    public bool Remove(K key)
    {
        throw new NotImplementedException();
    }

    public bool TryGetValue(K key, [MaybeNullWhen(false)] out V value)
    {
        for (int i = 0; i < keys.Count; i++)
        {
            if (keys[i].Equals(key))
            {
                value = values[i];
                return true;
            }
        }
        value = default;
        return false;
    }

    public void Add(KeyValuePair<K, V> item)
    {
        Add(item.Key, item.Value);
    }

    public void Clear()
    {
        keys.Clear();
        values.Clear();
    }

    public bool Contains(KeyValuePair<K, V> item)
    {
        for (int i = 0; i < keys.Count; i++)
        {
            if (keys[i].Equals(item.Key) && EqualityComparer<V>.Default.Equals(values[i], item.Value))
                return true;
        }
        return false;
    }

    public void CopyTo(KeyValuePair<K, V>[] array, int arrayIndex)
    {
        if (array == null)
            throw new ArgumentNullException(nameof(array));
        if (arrayIndex < 0 || arrayIndex + keys.Count > array.Length)
            throw new ArgumentOutOfRangeException(nameof(arrayIndex));
        
        for (int i = 0; i < keys.Count; i++)
        {
            array[arrayIndex + i] = new KeyValuePair<K, V>(keys[i], values[i]);
        }
    }

    public bool Remove(KeyValuePair<K, V> item)
    {
        throw new NotImplementedException();
    }

    public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
    {
        for (int i = 0; i < keys.Count; i++)
        {
            yield return new KeyValuePair<K, V>(keys[i], values[i]);
        }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }



    public readonly int Count => keys.Count;

    public readonly bool IsReadOnly => false;

    public readonly ICollection<K> Keys
    {
        get
        {
            return keys;
        }
    }

    public ICollection<V> Values
    {
        get
        {
            return values;
        }
    }

    public V this[K key]
    {
        get
        {
            if (TryGetValue(key, out var value))
                return value;
            throw new KeyNotFoundException($"Key '{key}' not found.");
        }
        set
        {
            for (int i = 0; i < keys.Count; i++)
            {
                if (keys[i].Equals(key))
                {
                    values[i] = value;
                    return;
                }
            }
            
            keys.Add(key);
            values.Add(value);
        }
    }
}