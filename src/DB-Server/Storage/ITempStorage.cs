using System.Collections.Generic;
using System.Runtime.InteropServices;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.Storage;


public class TempEntry<T>
{
    public T Value;
    public long Ts;
    public int Tid;

    public TempEntry(T value, long ts, int tid)
    {
        Value = value;
        Ts = ts;
        Tid = tid;
    }
}

public interface ITempTable<TKey, TValue>
{
    bool Get(TKey key, out TempEntry<TValue> entry);
    void Put(TKey key, TempEntry<TValue> entry);
    void Clear();
}


public interface ITempStorage
{
    // Table accessors - provide generic access to each table
    ITempTable<(int shard, string key), string> KV { get; }
    ITempTable<long, Warehouse> Warehouse { get; }
    ITempTable<(long, long), District> District { get; }
    ITempTable<(long, long, long), Customer> Customer { get; }
    ITempTable<long, Item> Item { get; }
    ITempTable<(long, long), Stock> Stock { get; }
    ITempTable<(long, long), History> History { get; }
    ITempTable<(long, long, long), NewOrder> NewOrder { get; }
    ITempTable<(long, long, long), Order> Order { get; }
    ITempTable<(long, long, long, long), OrderLine> OrderLine { get; }
}