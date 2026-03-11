using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Storage;


namespace Minerva.DB_Server.Transactions;

public class MinervaTx : Transaction
{
    public WriteSet WS = new();
    public ReadSet RS = new();
    public KeyAccessedFromSnapshotStore keyAccessedFromSnapshot = new();

    private readonly ReadsTimeStamp _reads = new();
    private readonly TimeProvider _timer;
    private readonly ReaderWriterLockSlim _rwLock;


    private readonly PersistentStorage _persistDb;
    private readonly ITempStorage _tempState;

    public HashSet<int> PrevTx { get; init; } = [];

    // For all the keys that were read by a transaction in this batch,
    // record the snapshot that the key was last updated in.
    // This is used to determine if a transaction is stale or not.
    public MinervaTx(int tid, PersistentStorage persistDb, ITempStorage tempState, TimeProvider timer, ReaderWriterLockSlim rwLock) : base(tid)
    {
        _persistDb = persistDb;
        _tempState = tempState;
        _timer = timer;
        _rwLock = rwLock;
    }

    public override void Abort()
    {
        Status = TransactionStatus.Aborted;
        throw new TransactionAbortedException($"Transaction {Tid} aborted by user request.");
    }

    public override void Begin()
    {
        Status = TransactionStatus.Active;
    }

    public override void Complete()
    {

        if (Status != TransactionStatus.Active)
        {
            throw new InvalidOperationException($"Transaction {Tid} is not active. Cannot complete.");
        }

        // OCC style validation using timestamps
        _rwLock.EnterWriteLock();

        try
        {
            if (!CheckNoConflict(WS.KVWriteSet, _reads.YCSBReadsTimeStamp, _tempState.KV) ||
                        !CheckNoConflict(WS.WarehouseWriteSet, _reads.WarehouseReadsTimeStamp, _tempState.Warehouse) ||
                        !CheckNoConflict(WS.DistrictWriteSet, _reads.DistrictReadsTimeStamp, _tempState.District) ||
                        !CheckNoConflict(WS.CustomerWriteSet, _reads.CustomerReadsTimeStamp, _tempState.Customer) ||
                        !CheckNoConflict(WS.ItemWriteSet, _reads.ItemReadsTimeStamp, _tempState.Item) ||
                        !CheckNoConflict(WS.StockWriteSet, _reads.StockReadsTimeStamp, _tempState.Stock) ||
                        !CheckNoConflict(WS.HistoryWriteSet, _reads.HistoryReadsTimeStamp, _tempState.History) ||
                        !CheckNoConflict(WS.NewOrderWriteSet, _reads.NewOrderReadsTimeStamp, _tempState.NewOrder) ||
                        !CheckNoConflict(WS.OrderWriteSet, _reads.OrderReadsTimeStamp, _tempState.Order) ||
                        !CheckNoConflict(WS.OrderLineWriteSet, _reads.OrderLineReadsTimeStamp, _tempState.OrderLine))
            {
                Status = TransactionStatus.Failed;
                throw new TransactionFailedException($"Transaction {Tid} aborted due to write conflict");
            }

            var curTime = _timer.GetUtcNow().Ticks;
            SaveToTempState(WS.KVWriteSet, _tempState.KV, curTime);
            SaveToTempState(WS.WarehouseWriteSet, _tempState.Warehouse, curTime);
            SaveToTempState(WS.DistrictWriteSet, _tempState.District, curTime);
            SaveToTempState(WS.CustomerWriteSet, _tempState.Customer, curTime);
            SaveToTempState(WS.ItemWriteSet, _tempState.Item, curTime);
            SaveToTempState(WS.StockWriteSet, _tempState.Stock, curTime);
            SaveToTempState(WS.HistoryWriteSet, _tempState.History, curTime);
            SaveToTempState(WS.NewOrderWriteSet, _tempState.NewOrder, curTime);
            SaveToTempState(WS.OrderWriteSet, _tempState.Order, curTime);
            SaveToTempState(WS.OrderLineWriteSet, _tempState.OrderLine, curTime);

        }
        finally
        {
            _rwLock.ExitWriteLock();
        }

        // Set the read and write keys
        _reads.ReadsTimeStampToReadSet(RS);

        Status = TransactionStatus.LocalCompleted;
    }




    private bool CheckNoConflict<TKey, TValue>(IDictionary<TKey, TValue> writes,
    IDictionary<TKey, long> reads,
    ITempTable<TKey, TValue> tempTable)
    {
        foreach (var (key, _) in writes)
        {
            if (reads.TryGetValue(key, out long readTimestamp))
            {
                if (tempTable.Get(key, out var entry))
                {
                    if (entry.Ts > readTimestamp)
                        return false;
                    PrevTx.Add(entry.Tid);
                }
            }
        }

        return true;
    }


    private void SaveToTempState<TKey, TValue>(IDictionary<TKey, TValue> writeSet,
        ITempTable<TKey, TValue> tempTable,
        long curTime)
    {
        foreach (var (key, value) in writeSet)
        {
            tempTable.Put(key, new TempEntry<TValue>(value, curTime, Tid));
        }
    }





    private void Put<TKey, TValue>(IDictionary<TKey, TValue> writeSet, TKey key, TValue value)
    {
        if (Status != TransactionStatus.Active)
        {
            throw new InvalidOperationException($"Transaction {Tid} is not active. Cannot set value for key '{key}'.");
        }

        writeSet[key] = value;
    }

    private bool Get<TKey, TValue>(TKey key,
    out TValue value,
    IDictionary<TKey, TValue> writeSet,
    Database database,
    ITempTable<TKey, TValue> tempTable,
    IDictionary<TKey, long> readsTs,
    IDictionary<TKey, int> snapshotTracker)
    {
        if (Status != TransactionStatus.Active)
        {
            throw new InvalidOperationException($"Transaction {Tid} is not active. Cannot get value for key '{key}'.");
        }

        // check if the key is already written in this transaction
        if (writeSet.TryGetValue(key, out value))
        {
            return true;
        }

        _rwLock.EnterReadLock();
        try
        {
            // check if is in the temporary state first
            if (tempTable.Get(key, out var entry))
            {
                value = entry.Value;
                readsTs[key] = entry.Ts;
                PrevTx.Add(entry.Tid);
                return true;
            }

            // get a copy from the persistent database
            if (_persistDb.Get(database, key, out PersistEntry<TValue> p1))
            {
                PersistEntry<TValue> temp = p1;
                value = temp.Value;
                // if its read from the persistent database, then it is not updated in the current epoch
                readsTs[key] = 0;
                // then we also need to track which snapshot it was read from
                snapshotTracker[key] = temp.Cid;
                return true;
            }

            value = default;
            return false;
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }



    // YCSB Ops
    public override void KeySet(int shard, string key, string value)
    {
        Put<(int, string), string>(WS.KVWriteSet, (shard, key), value);
    }

    public override bool KeyGet(int shard, string key, out string value)
    {
        return Get((shard, key), out value,
            WS.KVWriteSet,
            Database.KV,
            _tempState.KV,
            _reads.YCSBReadsTimeStamp,
            keyAccessedFromSnapshot.KVKeyAccessedFromSnapshot);
    }

    // TPC-C
    public override void PutOrder(Order order)
    {
        var key = (order.O_W_ID, order.O_D_ID, order.O_ID);

        Put(WS.OrderWriteSet, key, order);
    }

    public override void PutStock(Stock stock)
    {
        var key = (stock.S_W_ID, stock.S_I_ID);

        Put(WS.StockWriteSet, key, stock);
    }

    public override void PutOrderLine(OrderLine orderLine)
    {
        var key = (orderLine.OL_W_ID, orderLine.OL_D_ID, orderLine.OL_O_ID, orderLine.OL_NUMBER);

        Put(WS.OrderLineWriteSet, key, orderLine);
    }

    public override void PutCustomer(Customer customer)
    {
        var key = (customer.C_W_ID, customer.C_D_ID, customer.C_ID);

        Put(WS.CustomerWriteSet, key, customer);
    }

    public override void PutItem(Item item)
    {
        Put(WS.ItemWriteSet, item.I_ID, item);
    }

    public override void PutWarehouse(Warehouse warehouse)
    {
        Put(WS.WarehouseWriteSet, warehouse.W_ID, warehouse);
    }

    public override void PutDistrict(District district)
    {
        var key = (district.D_W_ID, district.D_ID);

        Put(WS.DistrictWriteSet, key, district);
    }

    public override void PutHistory(History history)
    {
        var key = (history.H_C_ID, history.H_DATE);

        Put(WS.HistoryWriteSet, key, history);
    }

    public override void PutNewOrder(NewOrder newOrder)
    {
        var key = (newOrder.NO_W_ID, newOrder.NO_D_ID, newOrder.NO_O_ID);

        Put(WS.NewOrderWriteSet, key, newOrder);
    }

    public override bool GetWarehouse(long warehouseId, out Warehouse warehouse)
    {
        return Get(warehouseId, out warehouse,
            WS.WarehouseWriteSet,
            Database.Warehouse,
            _tempState.Warehouse,
            _reads.WarehouseReadsTimeStamp,
            keyAccessedFromSnapshot.WarehouseReadsFromSnapshot);
    }

    public override bool GetDistrict(long warehouseId, long districtId, out District district)
    {
        var key = (warehouseId, districtId);

        return Get(key, out district,
            WS.DistrictWriteSet,
            Database.District,
            _tempState.District,
            _reads.DistrictReadsTimeStamp,
            keyAccessedFromSnapshot.DistrictReadsFromSnapshot);
    }

    public override bool GetCustomer(long warehouseId, long districtId, long customerId, out Customer customer)
    {
        var key = (warehouseId, districtId, customerId);

        return GetCustomerByID(key, out customer);

    }

    private bool GetCustomerByID((long, long, long) customerIdStr, out Customer customer)
    {
        return Get(customerIdStr, out customer,
            WS.CustomerWriteSet,
            Database.Customer,
            _tempState.Customer,
            _reads.CustomerReadsTimeStamp,
            keyAccessedFromSnapshot.CustomerReadsFromSnapshot);
    }

    public override bool GetCustomerByLastName(int warehouseId, int districtId, string lastName, out Customer customer)
    {
        var customerKey = (warehouseId, districtId, lastName);

        if (!_persistDb.CustomerIndexByLastName.TryGetValue(customerKey, out var customerList) || customerList.Count == 0)
        {
            customer = null;
            return false;
        }

        if (customerList.Count == 1)
        {
            GetCustomerByID(customerList.Last(), out customer);
            return true;
        }
        else
        {
            List<Customer> customers = [];
            foreach (var id in customerList)
            {
                if (GetCustomerByID(id, out var cust))
                {
                    customers.Add(cust);
                }
            }

            // Sort the customers by CID 
            // Select the customer with the middle C_ID
            customers.Sort((c1, c2) => c1.C_ID.CompareTo(c2.C_ID));
            int middleIndex = customers.Count / 2;
            if (customers.Count % 2 == 0)
            {
                middleIndex--;
            }
            customer = customers[middleIndex];
            return true;
        }
    }


    public override bool GetItem(long itemId, out Item item)
    {
        return Get(itemId, out item,
            WS.ItemWriteSet,
            Database.Item,
            _tempState.Item,
            _reads.ItemReadsTimeStamp,
            keyAccessedFromSnapshot.ItemReadsFromSnapshot);
    }

    public override bool GetStock(long warehouseId, long itemId, out Stock stock)
    {
        var key = (warehouseId, itemId);

        return Get(key, out stock,
            WS.StockWriteSet,
            Database.Stock,
            _tempState.Stock,
            _reads.StockReadsTimeStamp,
            keyAccessedFromSnapshot.StockReadsFromSnapshot);
    }

    public override bool GetHistory(long customerId, long timestamp, out History history)
    {
        var key = (customerId, timestamp);

        return Get(key, out history,
            WS.HistoryWriteSet,
            Database.History,
            _tempState.History,
            _reads.HistoryReadsTimeStamp,
            keyAccessedFromSnapshot.HistoryReadsFromSnapshot);
    }

    public override bool GetNewOrder(long warehouseId, long districtId, long orderId, out NewOrder newOrder)
    {
        var key = (warehouseId, districtId, orderId);

        return Get(key, out newOrder,
            WS.NewOrderWriteSet,
            Database.NewOrder,
            _tempState.NewOrder,
            _reads.NewOrderReadsTimeStamp,
            keyAccessedFromSnapshot.NewOrderReadsFromSnapshot);
    }

    public override bool GetOrder(long warehouseId, long districtId, long orderId, out Order order)
    {
        var key = (warehouseId, districtId, orderId);

        return Get(key, out order,
            WS.OrderWriteSet,
            Database.Order,
            _tempState.Order,
            _reads.OrderReadsTimeStamp,
            keyAccessedFromSnapshot.OrderReadsFromSnapshot);
    }

    public override bool GetOrderLine(long warehouseId, long districtId, long orderId, long orderLineNumber, out OrderLine orderLine)
    {
        var key = (warehouseId, districtId, orderId, orderLineNumber);

        return Get(key, out orderLine,
            WS.OrderLineWriteSet,
            Database.OrderLine,
            _tempState.OrderLine,
            _reads.OrderLineReadsTimeStamp,
            keyAccessedFromSnapshot.OrderLineReadsFromSnapshot);
    }

}
