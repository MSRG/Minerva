using System.Collections.Generic;
using System.Runtime.InteropServices;
using Minerva.DB_Server.Network.Protos;
using MemoryPack;

namespace Minerva.DB_Server.Storage;

public enum Database
{
    // YCSB
    KV,

    // TPC-C
    Warehouse,
    District,
    Customer,
    Item,
    Stock,
    History,
    NewOrder,
    Order,
    OrderLine
}

[MemoryPackable]
// Generic entry to hold value and last-updated commit id (Cid)
public partial class PersistEntry<T>
{
    public T Value;
    public int Cid;

    public PersistEntry(T value, int cid)
    {
        Value = value;
        Cid = cid;
    }
}


public interface IPersistentTable<TKey, TValue>
{
    bool Get(TKey key, out PersistEntry<TValue> entry);
    void Put(TKey key, PersistEntry<TValue> entry);
    void Clear();
}

public interface IPersistentTable<TKey, TValue, TInternalData> : IPersistentTable<TKey, TValue>
{
    TInternalData GetInternalData();
    void LoadFromData(TInternalData data);
}

public interface IPersistentStorage
{
    // Table accessors - provide generic access to each table
    IPersistentTable<(int shard, string key), string> YCSB { get; }
    IPersistentTable<long, Warehouse> Warehouse { get; }
    IPersistentTable<(long, long), District> District { get; }
    IPersistentTable<(long, long, long), Customer> Customer { get; }
    IPersistentTable<long, Item> Item { get; }
    IPersistentTable<(long, long), Stock> Stock { get; }
    IPersistentTable<(long, long), History> History { get; }
    IPersistentTable<(long, long, long), NewOrder> NewOrder { get; }
    IPersistentTable<(long, long, long), Order> Order { get; }
    IPersistentTable<(long, long, long, long), OrderLine> OrderLine { get; }
}