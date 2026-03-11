using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.Storage;

/// <summary>
/// A table accessor backed by a dictionary.
/// </summary>
public sealed class DictionaryTable<TKey, TValue> : ITempTable<TKey, TValue>
{
    private Dictionary<TKey, TempEntry<TValue>> _store = [];

    public bool Get(TKey key, out TempEntry<TValue> entry)
    {
        return _store.TryGetValue(key, out entry);
    }

    public void Put(TKey key, TempEntry<TValue> entry)
    {
        _store[key] = entry;
    }

    public void Clear() => _store.Clear();
}

/// <summary>
/// A sharded table accessor backed by an array of dictionaries, indexed by shard id.
/// </summary>
public sealed class ShardedDictionaryTable : ITempTable<(int shard, string key), string>
{
    private readonly Dictionary<string, TempEntry<string>>[] _shards;

    public ShardedDictionaryTable(int shardCount)
    {
        _shards = new Dictionary<string, TempEntry<string>>[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            _shards[i] =  [];
        }
    }

    public bool Get((int shard, string key) key, out TempEntry<string> entry)
    {
        return _shards[key.shard].TryGetValue(key.key, out entry);
    }

    public void Put((int shard, string key) key, TempEntry<string> entry)
    {
        _shards[key.shard][key.key] = entry;
    }

    public void Clear()
    {
        foreach (var shard in _shards)
        {
            shard.Clear();
        }
    }
}

/// <summary>
/// Warehouse table: 1D array indexed by W_ID (fixed size)
/// </summary>
public sealed class TWarehouseTable : ITempTable<long, Warehouse>
{
    private readonly TempEntry<Warehouse>[] _data;

    public TWarehouseTable(int maxWarehouses)
    {
        _data = new TempEntry<Warehouse>[maxWarehouses];
    }

    public bool Get(long key, out TempEntry<Warehouse> entry)
    {
        entry = _data[key - 1];
        return entry != null;
    }

    public void Put(long key, TempEntry<Warehouse> entry) => _data[key - 1] = entry;

    public void Clear() => Array.Clear(_data);
}

/// <summary>
/// Item table: 1D array indexed by I_ID (fixed size)
/// </summary>
public sealed class TItemTable : ITempTable<long, Item>
{
    private readonly TempEntry<Item>[] _data;

    public TItemTable(int maxItems)
    {
        _data = new TempEntry<Item>[maxItems];
    }

    public bool Get(long key, out TempEntry<Item> entry)
    {
        entry = _data[key - 1];
        return entry != null;
    }

    public void Put(long key, TempEntry<Item> entry) => _data[key - 1] = entry;

    public void Clear() => Array.Clear(_data);
}


/// <summary>
/// District table: 2D array indexed by [W_ID, D_ID] (fixed size: 10 districts per warehouse)
/// </summary>
public sealed class TDistrictTable : ITempTable<(long, long), District>
{
    private readonly TempEntry<District>[,] _data;

    public TDistrictTable(int maxWarehouses, int districtsPerWarehouse)
    {
        _data = new TempEntry<District>[maxWarehouses, districtsPerWarehouse];
    }

    public bool Get((long, long) key, out TempEntry<District> entry)
    {
        entry = _data[key.Item1 - 1, key.Item2 - 1];
        return entry != null;
    }

    public void Put((long, long) key, TempEntry<District> entry) => _data[key.Item1 - 1, key.Item2 - 1] = entry;

    public void Clear() => Array.Clear(_data);
}


/// <summary>
/// Customer table: 2D array indexed by [W_ID, D_ID] with Dictionary for C_ID
/// </summary>
public sealed class TCustomerTable : ITempTable<(long, long, long), Customer>
{
    private readonly Dictionary<long, TempEntry<Customer>>[,] _data;

    public TCustomerTable(int maxWarehouses, int districtsPerWarehouse)
    {
        _data = new Dictionary<long, TempEntry<Customer>>[maxWarehouses, districtsPerWarehouse];
        for (int w = 0; w < maxWarehouses; w++)
        {
            for (int d = 0; d < districtsPerWarehouse; d++)
            {
                _data[w, d] = [];
            }
        }
    }

    public bool Get((long, long, long) key, out TempEntry<Customer> entry) =>
        _data[key.Item1 - 1, key.Item2 - 1].TryGetValue(key.Item3, out entry);

    public void Put((long, long, long) key, TempEntry<Customer> entry) =>
        _data[key.Item1 - 1, key.Item2 - 1][key.Item3] = entry;

    public void Clear()
    {
        foreach (var dict in _data)
        {
            dict.Clear();
        }
    }
}

/// <summary>
/// NewOrder table: 2D array [W_ID][D_ID] -> Dictionary by O_ID
/// NewOrders grow and shrink as orders are placed and delivered
/// </summary>
public sealed class TNewOrderTable : ITempTable<(long, long, long), NewOrder>
{
    private readonly Dictionary<long, TempEntry<NewOrder>>[,] _data;

    public TNewOrderTable(int maxWarehouses, int districtsPerWarehouse)
    {
        _data = new Dictionary<long, TempEntry<NewOrder>>[maxWarehouses, districtsPerWarehouse];
        for (int w = 0; w < maxWarehouses; w++)
        {
            for (int d = 0; d < districtsPerWarehouse; d++)
            {
                _data[w, d] = [];
            }
        }
    }

    public bool Get((long, long, long) key, out TempEntry<NewOrder> entry)
    {
        var orders = _data[key.Item1 - 1, key.Item2 - 1];
        return orders.TryGetValue(key.Item3, out entry);
    }

    public void Put((long, long, long) key, TempEntry<NewOrder> entry)
    {
        var orders = _data[key.Item1 - 1, key.Item2 - 1];
        orders[key.Item3] = entry;
    }

    public void Clear()
    {
        foreach (var dict in _data)
        {
            dict.Clear();
        }
    }
}


/// <summary>
/// Order table: 2D array [W_ID][D_ID] -> Dictionary by O_ID
/// Orders grow over time as new orders are placed
/// </summary>
public sealed class TOrderTable : ITempTable<(long, long, long), Order>
{
    private readonly Dictionary<long, TempEntry<Order>>[,] _data;

    public TOrderTable(int maxWarehouses, int districtsPerWarehouse)
    {
        _data = new Dictionary<long, TempEntry<Order>>[maxWarehouses, districtsPerWarehouse];
        for (int w = 0; w < maxWarehouses; w++)
        {
            for (int d = 0; d < districtsPerWarehouse; d++)
            {
                _data[w, d] = [];
            }
        }
    }

    public bool Get((long, long, long) key, out TempEntry<Order> entry)
    {
        var orders = _data[key.Item1 - 1, key.Item2 - 1];
        return orders.TryGetValue(key.Item3, out entry);
    }

    public void Put((long, long, long) key, TempEntry<Order> entry)
    {
        var orders = _data[key.Item1 - 1, key.Item2 - 1];
        orders[key.Item3] = entry;
    }

    public void Clear()
    {
        foreach (var dict in _data)
        {
            dict.Clear();
        }
    }
}






public class TempStorage : ITempStorage
{
    // Table accessors
    public ITempTable<(int shard, string key), string> KV { get; } = new ShardedDictionaryTable(Consts.Storage.MAX_YCSB_SHARDS);
    
    // TPC-C tables with appropriate backing structures
    public ITempTable<long, Warehouse> Warehouse { get; } = new TWarehouseTable(Consts.Storage.MAX_TPCC_WAREHOUSE);
    public ITempTable<long, Item> Item { get; } = new TItemTable(Consts.Storage.MAX_ITEMS);
    public ITempTable<(long, long), District> District { get; } = new TDistrictTable(Consts.Storage.MAX_TPCC_WAREHOUSE, Consts.Storage.DISTRICTS_PER_WAREHOUSE);
    public ITempTable<(long, long, long), Customer> Customer { get; } = new TCustomerTable(Consts.Storage.MAX_TPCC_WAREHOUSE, Consts.Storage.DISTRICTS_PER_WAREHOUSE);
    public ITempTable<(long, long), Stock> Stock { get; } = new DictionaryTable<(long, long), Stock>();
    public ITempTable<(long, long), History> History { get; } = new DictionaryTable<(long, long), History>();
    public ITempTable<(long, long, long), NewOrder> NewOrder { get; } = new TNewOrderTable(Consts.Storage.MAX_TPCC_WAREHOUSE, Consts.Storage.DISTRICTS_PER_WAREHOUSE);
    public ITempTable<(long, long, long), Order> Order { get; } = new TOrderTable(Consts.Storage.MAX_TPCC_WAREHOUSE, Consts.Storage.DISTRICTS_PER_WAREHOUSE);
    public ITempTable<(long, long, long, long), OrderLine> OrderLine { get; } = new DictionaryTable<(long, long, long, long), OrderLine>();

    public void Clear()
    {
        KV.Clear();
        Warehouse.Clear();
        District.Clear();
        Customer.Clear();
        Item.Clear();
        Stock.Clear();
        History.Clear();
        NewOrder.Clear();
        Order.Clear();
        OrderLine.Clear();
    }
}