using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MemoryPack;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.Storage;

/// <summary>
/// A sharded table accessor backed by an array of dictionaries, indexed by shard id.
/// </summary>
public sealed class PShardedDictionaryTable : IPersistentTable<(int shard, string key), string, Dictionary<string, PersistEntry<string>>[]>
{
    private readonly Dictionary<string, PersistEntry<string>>[] _shards;

    public PShardedDictionaryTable(int shardCount)
    {
        _shards = new Dictionary<string, PersistEntry<string>>[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            _shards[i] = new Dictionary<string, PersistEntry<string>>(Consts.Storage.INIT_KV_TABLE_CAP);
        }
    }

    public bool Get((int shard, string key) key, out PersistEntry<string> entry)
    {
        return _shards[key.shard].TryGetValue(key.key, out entry);
    }

    public void Put((int shard, string key) key, PersistEntry<string> entry)
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

    public Dictionary<string, PersistEntry<string>>[] GetInternalData() => _shards;

    public void LoadFromData(Dictionary<string, PersistEntry<string>>[] data)
    {
        Clear();
        for (int i = 0; i < Math.Min(data.Length, _shards.Length); i++)
        {
            foreach (var kvp in data[i])
            {
                _shards[i][kvp.Key] = kvp.Value;
            }
        }
    }
}

/// <summary>
/// Warehouse table: 1D array indexed by W_ID (fixed size)
/// </summary>
public sealed class WarehouseTable : IPersistentTable<long, Warehouse, PersistEntry<Warehouse>[]>
{
    private readonly PersistEntry<Warehouse>[] _data;

    public WarehouseTable(int maxWarehouses)
    {
        _data = new PersistEntry<Warehouse>[maxWarehouses];
    }

    public bool Get(long key, out PersistEntry<Warehouse> entry)
    {
        entry = _data[key - 1];
        return entry != null;
    }

    public void Put(long key, PersistEntry<Warehouse> entry) => _data[key - 1] = entry;

    public void Clear() => Array.Clear(_data);

    public PersistEntry<Warehouse>[] GetInternalData() => _data;

    public void LoadFromData(PersistEntry<Warehouse>[] data)
    {
        Clear();
        Array.Copy(data, _data, Math.Min(data.Length, _data.Length));
    }
}


/// <summary>
/// Item table: 1D array indexed by I_ID (fixed size)
/// </summary>
public sealed class ItemTable : IPersistentTable<long, Item, PersistEntry<Item>[]>
{
    private readonly PersistEntry<Item>[] _data;

    public ItemTable(int maxItems)
    {
        _data = new PersistEntry<Item>[maxItems];
    }

    public bool Get(long key, out PersistEntry<Item> entry)
    {
        entry = _data[key - 1];
        return entry != null;
    }

    public void Put(long key, PersistEntry<Item> entry) => _data[key - 1] = entry;

    public void Clear() => Array.Clear(_data);

    public PersistEntry<Item>[] GetInternalData() => _data;

    public void LoadFromData(PersistEntry<Item>[] data)
    {
        Clear();
        Array.Copy(data, _data, Math.Min(data.Length, _data.Length));
    }
}

/// <summary>
/// District table: jagged array indexed by [W_ID][D_ID] (fixed size: 10 districts per warehouse)
/// </summary>
public sealed class DistrictTable : IPersistentTable<(long, long), District, PersistEntry<District>[][]>
{
    private readonly PersistEntry<District>[][] _data;

    public DistrictTable(int maxWarehouses, int districtsPerWarehouse)
    {
        _data = new PersistEntry<District>[maxWarehouses][];
        for (int i = 0; i < maxWarehouses; i++)
        {
            _data[i] = new PersistEntry<District>[districtsPerWarehouse];
        }
    }

    public bool Get((long, long) key, out PersistEntry<District> entry)
    {
        entry = _data[key.Item1 - 1][key.Item2 - 1];
        return entry != null;
    }

    public void Put((long, long) key, PersistEntry<District> entry) => _data[key.Item1 - 1][key.Item2 - 1] = entry;

    public void Clear()
    {
        foreach (var arr in _data)
        {
            Array.Clear(arr);
        }
    }

    public PersistEntry<District>[][] GetInternalData() => _data;

    public void LoadFromData(PersistEntry<District>[][] data)
    {
        Clear();
        for (int i = 0; i < Math.Min(data.Length, _data.Length); i++)
        {
            Array.Copy(data[i], _data[i], Math.Min(data[i].Length, _data[i].Length));
        }
    }
}


/// <summary>
/// Customer table: jagged array indexed by [W_ID][D_ID][C_ID] (fixed size: 3000 customers per district)
/// </summary>
public sealed class CustomerTable : IPersistentTable<(long, long, long), Customer, PersistEntry<Customer>[][][]>
{
    private readonly PersistEntry<Customer>[][][] _data;

    public CustomerTable(int maxWarehouses, int districtsPerWarehouse, int customersPerDistrict)
    {
        _data = new PersistEntry<Customer>[maxWarehouses][][];
        for (int w = 0; w < maxWarehouses; w++)
        {
            _data[w] = new PersistEntry<Customer>[districtsPerWarehouse][];
            for (int d = 0; d < districtsPerWarehouse; d++)
            {
                _data[w][d] = new PersistEntry<Customer>[customersPerDistrict];
            }
        }
    }

    public bool Get((long, long, long) key, out PersistEntry<Customer> entry)
    {
        entry = _data[key.Item1 - 1][key.Item2 - 1][key.Item3 - 1];
        return entry != null;
    }

    public void Put((long, long, long) key, PersistEntry<Customer> entry) => _data[key.Item1 - 1][key.Item2 - 1][key.Item3 - 1] = entry;

    public void Clear()
    {
        foreach (var warehouse in _data)
        {
            foreach (var district in warehouse)
            {
                Array.Clear(district);
            }
        }
    }

    public PersistEntry<Customer>[][][] GetInternalData() => _data;

    public void LoadFromData(PersistEntry<Customer>[][][] data)
    {
        Clear();
        for (int w = 0; w < Math.Min(data.Length, _data.Length); w++)
        {
            for (int d = 0; d < Math.Min(data[w].Length, _data[w].Length); d++)
            {
                Array.Copy(data[w][d], _data[w][d], Math.Min(data[w][d].Length, _data[w][d].Length));
            }
        }
    }
}

/// <summary>
/// Stock table: jagged array [W_ID][I_ID] (fixed 100K items per warehouse)
/// Using jagged array to avoid huge contiguous allocation
/// </summary>
public sealed class StockTable : IPersistentTable<(long, long), Stock, PersistEntry<Stock>[][]>
{
    private readonly PersistEntry<Stock>[][] _data;

    public StockTable(int maxWarehouses, int maxItems)
    {
        _data = new PersistEntry<Stock>[maxWarehouses][];
        for (int i = 0; i < maxWarehouses; i++)
        {
            _data[i] = new PersistEntry<Stock>[maxItems];
        }
    }

    public bool Get((long, long) key, out PersistEntry<Stock> entry)
    {
        entry = _data[key.Item1 - 1][key.Item2 - 1];
        return entry != null;
    }

    public void Put((long, long) key, PersistEntry<Stock> entry) => _data[key.Item1 - 1][key.Item2 - 1] = entry;

    public void Clear()
    {
        foreach (var arr in _data)
        {
            Array.Clear(arr);
        }
    }

    public PersistEntry<Stock>[][] GetInternalData() => _data;

    public void LoadFromData(PersistEntry<Stock>[][] data)
    {
        Clear();
        for (int i = 0; i < Math.Min(data.Length, _data.Length); i++)
        {
            Array.Copy(data[i], _data[i], Math.Min(data[i].Length, _data[i].Length));
        }
    }
}

/// <summary>
/// History table: jagged list [W_ID][D_ID] -> Dictionary by (C_ID, H_DATE)
/// History grows over time, H_DATE is a timestamp
/// </summary>
public sealed class HistoryTable : IPersistentTable<(long, long), History, Dictionary<(long, long), PersistEntry<History>>>
{
    private readonly Dictionary<(long, long), PersistEntry<History>> _data = [];

    public bool Get((long, long) key, out PersistEntry<History> entry)
    {
        return _data.TryGetValue(key, out entry);
    }

    public void Put((long, long) key, PersistEntry<History> entry) => _data[key] = entry;

    public void Clear() => _data.Clear();

    public Dictionary<(long, long), PersistEntry<History>> GetInternalData() => _data;

    public void LoadFromData(Dictionary<(long, long), PersistEntry<History>> data)
    {
        Clear();
        foreach (var kvp in data)
        {
            _data[kvp.Key] = kvp.Value;
        }
    }
}

/// <summary>
/// NewOrder table: jagged array [W_ID][D_ID] -> List by O_ID
/// NewOrders grow and shrink as orders are placed and delivered
/// </summary>
public sealed class NewOrderTable : IPersistentTable<(long, long, long), NewOrder, List<PersistEntry<NewOrder>>[][]>
{
    private readonly List<PersistEntry<NewOrder>>[][] _data;

    public NewOrderTable(int maxWarehouses, int districtsPerWarehouse)
    {
        _data = new List<PersistEntry<NewOrder>>[maxWarehouses][];
        for (int w = 0; w < maxWarehouses; w++)
        {
            _data[w] = new List<PersistEntry<NewOrder>>[districtsPerWarehouse];
            for (int d = 0; d < districtsPerWarehouse; d++)
            {
                _data[w][d] = [];
            }
        }
    }

    public bool Get((long, long, long) key, out PersistEntry<NewOrder> entry)
    {
        var orders = _data[key.Item1 - 1][key.Item2 - 1];
        var idx = (int)(key.Item3 - 1);
        entry = idx >= 0 && idx < orders.Count ? orders[idx] : null;
        return entry != null;
    }

    public void Put((long, long, long) key, PersistEntry<NewOrder> entry)
    {
        var orders = _data[key.Item1 - 1][key.Item2 - 1];
        var idx = (int)(key.Item3 - 1);

        while (orders.Count <= idx)
        {
            orders.Add(null);
        }
        orders[idx] = entry;
    }

    public void Clear()
    {
        foreach (var warehouse in _data)
        {
            foreach (var district in warehouse)
            {
                district.Clear();
            }
        }
    }

    public List<PersistEntry<NewOrder>>[][] GetInternalData() => _data;

    public void LoadFromData(List<PersistEntry<NewOrder>>[][] data)
    {
        Clear();
        for (int w = 0; w < Math.Min(data.Length, _data.Length); w++)
        {
            for (int d = 0; d < Math.Min(data[w].Length, _data[w].Length); d++)
            {
                _data[w][d].AddRange(data[w][d]);
            }
        }
    }
}

/// <summary>
/// Order table: jagged array [W_ID][D_ID] -> List by O_ID
/// Orders grow over time as new orders are placed
/// </summary>
public sealed class OrderTable : IPersistentTable<(long, long, long), Order, List<PersistEntry<Order>>[][]>
{
    private readonly List<PersistEntry<Order>>[][] _data;

    public OrderTable(int maxWarehouses, int districtsPerWarehouse)
    {
        _data = new List<PersistEntry<Order>>[maxWarehouses][];
        for (int w = 0; w < maxWarehouses; w++)
        {
            _data[w] = new List<PersistEntry<Order>>[districtsPerWarehouse];
            for (int d = 0; d < districtsPerWarehouse; d++)
            {
                _data[w][d] = [];
            }
        }
    }

    public bool Get((long, long, long) key, out PersistEntry<Order> entry)
    {
        var orders = _data[key.Item1 - 1][key.Item2 - 1];
        var idx = (int)(key.Item3 - 1);
        entry = idx >= 0 && idx < orders.Count ? orders[idx] : null;
        return entry != null;
    }

    public void Put((long, long, long) key, PersistEntry<Order> entry)
    {
        var orders = _data[key.Item1 - 1][key.Item2 - 1];
        var idx = (int)(key.Item3 - 1);

        while (orders.Count <= idx)
        {
            orders.Add(null);
        }
        orders[idx] = entry;
    }

    public void Clear()
    {
        foreach (var warehouse in _data)
        {
            foreach (var district in warehouse)
            {
                district.Clear();
            }
        }
    }

    public List<PersistEntry<Order>>[][] GetInternalData() => _data;

    public void LoadFromData(List<PersistEntry<Order>>[][] data)
    {
        Clear();
        for (int w = 0; w < Math.Min(data.Length, _data.Length); w++)
        {
            for (int d = 0; d < Math.Min(data[w].Length, _data[w].Length); d++)
            {
                _data[w][d].AddRange(data[w][d]);
            }
        }
    }
}

/// <summary>
/// OrderLine table: jagged array [W_ID][D_ID] -> List[O_ID] -> List by OL_NUMBER
/// OrderLines grow with orders, 5-15 lines per order
/// </summary>
public sealed class OrderLineTable : IPersistentTable<(long, long, long, long), OrderLine, List<List<PersistEntry<OrderLine>>>[][]>
{
    private readonly List<List<PersistEntry<OrderLine>>>[][] _data;

    public OrderLineTable(int maxWarehouses, int districtsPerWarehouse)
    {
        _data = new List<List<PersistEntry<OrderLine>>>[maxWarehouses][];
        for (int w = 0; w < maxWarehouses; w++)
        {
            _data[w] = new List<List<PersistEntry<OrderLine>>>[districtsPerWarehouse];
            for (int d = 0; d < districtsPerWarehouse; d++)
            {
                _data[w][d] = [];
            }
        }
    }

    public bool Get((long, long, long, long) key, out PersistEntry<OrderLine> entry)
    {
        var orders = _data[key.Item1 - 1][key.Item2 - 1];
        var oIdx = (int)(key.Item3 - 1);
        var olIdx = (int)(key.Item4 - 1);
        if (oIdx < 0 || oIdx >= orders.Count)
        {
            entry = null;
            return false;
        }
        var lines = orders[oIdx];
        if (lines == null || olIdx < 0 || olIdx >= lines.Count)
        {
            entry = null;
            return false;
        }
        entry = lines[olIdx];
        return entry != null;
    }

    public void Put((long, long, long, long) key, PersistEntry<OrderLine> entry)
    {
        var orders = _data[key.Item1 - 1][key.Item2 - 1];
        var oIdx = (int)(key.Item3 - 1);
        var olIdx = (int)(key.Item4 - 1);

        while (orders.Count <= oIdx)
        {
            orders.Add(null);
        }
        if (orders[oIdx] == null)
        {
            orders[oIdx] = [];
        }
        var lines = orders[oIdx];
        while (lines.Count <= olIdx)
        {
            lines.Add(null);
        }
        lines[olIdx] = entry;
    }

    public void Clear()
    {
        foreach (var warehouse in _data)
        {
            foreach (var district in warehouse)
            {
                district.Clear();
            }
        }
    }

    public List<List<PersistEntry<OrderLine>>>[][] GetInternalData() => _data;

    public void LoadFromData(List<List<PersistEntry<OrderLine>>>[][] data)
    {
        Clear();
        for (int w = 0; w < Math.Min(data.Length, _data.Length); w++)
        {
            for (int d = 0; d < Math.Min(data[w].Length, _data[w].Length); d++)
            {
                foreach (var orderLines in data[w][d])
                {
                    _data[w][d].Add(orderLines != null ? [.. orderLines] : null);
                }
            }
        }
    }
}

public class PersistentStorage
{

    // YCSB
    public IPersistentTable<(int shard, string key), string> KV => _kv;
    private readonly PShardedDictionaryTable _kv = new(Consts.Storage.MAX_YCSB_SHARDS);

    // TPC-C
    public IPersistentTable<long, Warehouse> Warehouse => _warehouse;
    private readonly WarehouseTable _warehouse = new(Consts.Storage.MAX_TPCC_WAREHOUSE);

    public IPersistentTable<long, Item> Item => _item;
    private readonly ItemTable _item = new(Consts.Storage.MAX_ITEMS);

    public IPersistentTable<(long, long), District> District => _district;
    private readonly DistrictTable _district = new(Consts.Storage.MAX_TPCC_WAREHOUSE, Consts.Storage.DISTRICTS_PER_WAREHOUSE);

    public IPersistentTable<(long, long, long), Customer> Customer => _customer;
    private readonly CustomerTable _customer = new(Consts.Storage.MAX_TPCC_WAREHOUSE, Consts.Storage.DISTRICTS_PER_WAREHOUSE, Consts.Storage.CUSTOMERS_PER_DISTRICT);

    public IPersistentTable<(long, long), Stock> Stock => _stock;
    private readonly StockTable _stock = new(Consts.Storage.MAX_TPCC_WAREHOUSE, Consts.Storage.MAX_ITEMS);

    public IPersistentTable<(long, long), History> History => _history;
    private readonly HistoryTable _history = new();

    public IPersistentTable<(long, long, long), NewOrder> NewOrder => _newOrder;
    private readonly NewOrderTable _newOrder = new(Consts.Storage.MAX_TPCC_WAREHOUSE, Consts.Storage.DISTRICTS_PER_WAREHOUSE);

    public IPersistentTable<(long, long, long), Order> Order => _order;
    private readonly OrderTable _order = new(Consts.Storage.MAX_TPCC_WAREHOUSE, Consts.Storage.DISTRICTS_PER_WAREHOUSE);

    public IPersistentTable<(long, long, long, long), OrderLine> OrderLine => _orderLine;
    private readonly OrderLineTable _orderLine = new(Consts.Storage.MAX_TPCC_WAREHOUSE, Consts.Storage.DISTRICTS_PER_WAREHOUSE);


    // for customer last name index C_W_ID, C_D_ID, C_LAST -> set of (C_W_ID, C_D_ID, C_ID)
    public Dictionary<(long, long, string), HashSet<(long CWID, long CDID, long CID)>> CustomerIndexByLastName = [];
    // For stale tracking
    public HashSet<(int rid, int tid)> ReExOriginalTransactions = [];



    public PersistentStorage()
    {
    }

    public bool Get<Tkey, TValue>(Database database, Tkey key, out PersistEntry<TValue> entry)
    {
        entry = default;


        switch (database)
        {
            case Database.KV:
                return ((IPersistentTable<Tkey, TValue>)KV).Get(key, out entry);

            case Database.Warehouse:
                return ((IPersistentTable<Tkey, TValue>)Warehouse).Get(key, out entry);

            case Database.District:
                return ((IPersistentTable<Tkey, TValue>)District).Get(key, out entry);

            case Database.Customer:
                return ((IPersistentTable<Tkey, TValue>)Customer).Get(key, out entry);

            case Database.Item:
                return ((IPersistentTable<Tkey, TValue>)Item).Get(key, out entry);

            case Database.Stock:
                return ((IPersistentTable<Tkey, TValue>)Stock).Get(key, out entry);

            case Database.History:
                return ((IPersistentTable<Tkey, TValue>)History).Get(key, out entry);

            case Database.NewOrder:
                return ((IPersistentTable<Tkey, TValue>)NewOrder).Get(key, out entry);

            case Database.Order:
                return ((IPersistentTable<Tkey, TValue>)Order).Get(key, out entry);

            case Database.OrderLine:
                return ((IPersistentTable<Tkey, TValue>)OrderLine).Get(key, out entry);

            default:
                throw new ArgumentOutOfRangeException(nameof(database), database, null);
        }

    }

    public void Set<TKey, TValue>(Database database, TKey key, PersistEntry<TValue> value)
    {
        switch (database)
        {
            case Database.KV:
                ((IPersistentTable<TKey, TValue>)KV).Put(key, value);
                return;

            case Database.Warehouse:
                ((IPersistentTable<TKey, TValue>)Warehouse).Put(key, value);
                return;
            case Database.District:
                ((IPersistentTable<TKey, TValue>)District).Put(key, value);
                return;

            case Database.Customer:
                ((IPersistentTable<TKey, TValue>)Customer).Put(key, value);
                return;

            case Database.Item:
                ((IPersistentTable<TKey, TValue>)Item).Put(key, value);
                return;

            case Database.Stock:
                ((IPersistentTable<TKey, TValue>)Stock).Put(key, value);
                return;

            case Database.History:
                ((IPersistentTable<TKey, TValue>)History).Put(key, value);
                return;

            case Database.NewOrder:
                ((IPersistentTable<TKey, TValue>)NewOrder).Put(key, value);
                return;

            case Database.Order:
                ((IPersistentTable<TKey, TValue>)Order).Put(key, value);
                return;

            case Database.OrderLine:
                ((IPersistentTable<TKey, TValue>)OrderLine).Put(key, value);
                return;

            default:
                throw new ArgumentOutOfRangeException(nameof(database), database, null);
        }
    }

    public void SaveStorageToDisk(string dirPath)
    {
        Directory.CreateDirectory(dirPath);

        SerializeToFile(dirPath, "YCSBStore.bin", _kv.GetInternalData());
        SerializeToFile(dirPath, "WarehouseStore.bin", _warehouse.GetInternalData());
        SerializeToFile(dirPath, "DistrictStore.bin", _district.GetInternalData());
        SerializeToFile(dirPath, "CustomerStore.bin", _customer.GetInternalData());
        SerializeToFile(dirPath, "ItemStore.bin", _item.GetInternalData());
        SerializeToFile(dirPath, "StockStore.bin", _stock.GetInternalData());
        SerializeToFile(dirPath, "HistoryStore.bin", _history.GetInternalData());
        SerializeToFile(dirPath, "NewOrderStore.bin", _newOrder.GetInternalData());
        SerializeToFile(dirPath, "OrderStore.bin", _order.GetInternalData());
        SerializeToFile(dirPath, "OrderLineStore.bin", _orderLine.GetInternalData());
        SerializeToFile(dirPath, "CustomerIndexByLastName.bin", CustomerIndexByLastName);
    }

    public async Task LoadStorageFromDisk(string dirPath, string[] databasesToLoad)
    {
        var tasks = new List<Task>();
        Task<Dictionary<string, PersistEntry<string>>[]> kvTask = null;
        Task<PersistEntry<Warehouse>[]> warehouseTask = null;
        Task<PersistEntry<District>[][]> districtTask = null;
        Task<PersistEntry<Customer>[][][]> customerTask = null;
        Task<PersistEntry<Item>[]> itemTask = null;
        Task<PersistEntry<Stock>[][]> stockTask = null;
        Task<Dictionary<(long, long), PersistEntry<History>>> historyTask = null;
        Task<List<PersistEntry<NewOrder>>[][]> newOrderTask = null;
        Task<List<PersistEntry<Order>>[][]> orderTask = null;
        Task<List<List<PersistEntry<OrderLine>>>[][]> orderLineTask = null;
        Task<Dictionary<(long, long, string), HashSet<(long, long, long)>>> customerIndexTask = null;

        if (databasesToLoad.Contains("YCSB"))
        {
            kvTask = DeserializeFromFile<Dictionary<string, PersistEntry<string>>[]>(dirPath, "YCSBStore.bin");
            tasks.Add(kvTask);
        }

        if (databasesToLoad.Contains("TPCC"))
        {
            warehouseTask = DeserializeFromFile<PersistEntry<Warehouse>[]>(dirPath, "WarehouseStore.bin");
            districtTask = DeserializeFromFile<PersistEntry<District>[][]>(dirPath, "DistrictStore.bin");
            customerTask = DeserializeFromFile<PersistEntry<Customer>[][][]>(dirPath, "CustomerStore.bin");
            itemTask = DeserializeFromFile<PersistEntry<Item>[]>(dirPath, "ItemStore.bin");
            stockTask = DeserializeFromFile<PersistEntry<Stock>[][]>(dirPath, "StockStore.bin");
            historyTask = DeserializeFromFile<Dictionary<(long, long), PersistEntry<History>>>(dirPath, "HistoryStore.bin");
            newOrderTask = DeserializeFromFile<List<PersistEntry<NewOrder>>[][]>(dirPath, "NewOrderStore.bin");
            orderTask = DeserializeFromFile<List<PersistEntry<Order>>[][]>(dirPath, "OrderStore.bin");
            orderLineTask = DeserializeFromFile<List<List<PersistEntry<OrderLine>>>[][]>(dirPath, "OrderLineStore.bin");
            customerIndexTask = DeserializeFromFile<Dictionary<(long, long, string), HashSet<(long, long, long)>>>(dirPath, "CustomerIndexByLastName.bin");
            tasks.AddRange(new Task[] {
                warehouseTask, districtTask, customerTask, itemTask, stockTask,
                historyTask, newOrderTask, orderTask, orderLineTask, customerIndexTask
            });
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        if (kvTask != null) _kv.LoadFromData(await kvTask.ConfigureAwait(false));
        if (warehouseTask != null) _warehouse.LoadFromData(await warehouseTask.ConfigureAwait(false));
        if (districtTask != null) _district.LoadFromData(await districtTask.ConfigureAwait(false));
        if (customerTask != null) _customer.LoadFromData(await customerTask.ConfigureAwait(false));
        if (itemTask != null) _item.LoadFromData(await itemTask.ConfigureAwait(false));
        if (stockTask != null) _stock.LoadFromData(await stockTask.ConfigureAwait(false));
        if (historyTask != null) _history.LoadFromData(await historyTask.ConfigureAwait(false));
        if (newOrderTask != null) _newOrder.LoadFromData(await newOrderTask.ConfigureAwait(false));
        if (orderTask != null) _order.LoadFromData(await orderTask.ConfigureAwait(false));
        if (orderLineTask != null) _orderLine.LoadFromData(await orderLineTask.ConfigureAwait(false));
        if (customerIndexTask != null) CustomerIndexByLastName = await customerIndexTask.ConfigureAwait(false);
    }

    private static void SerializeToFile<T>(string directory, string fileName, T data)
    {
        var path = Path.Combine(directory, fileName);
        using var stream = File.Create(path);
        MemoryPackSerializer.SerializeAsync(stream, data).AsTask().GetAwaiter().GetResult();
    }

    private static async Task<T> DeserializeFromFile<T>(string directory, string fileName)
    {
        var path = Path.Combine(directory, fileName);
        using var stream = File.OpenRead(path);
        return await MemoryPackSerializer.DeserializeAsync<T>(stream) ?? throw new InvalidDataException($"Failed to deserialize {fileName}");
    }
}