using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Storage;
using static Minerva.DB_Server.Storage.PersistentStorage;

namespace Minerva.DB_Server.Transactions;


public static class TxStateHelpers
{

    private static Dictionary<Database, object> TempStorageLocks = new()
    {
        { Database.KV, new object() },
        { Database.Warehouse, new object() },
        { Database.District, new object() },
        { Database.Customer, new object() },
        { Database.Item, new object() },
        { Database.Stock, new object() },
        { Database.History, new object() },
        { Database.NewOrder, new object() },
        { Database.Order, new object() },
        { Database.OrderLine, new object() },
    };

    public static void ApplyWriteSetToPersistentDB(WriteSet writeSet, PersistentStorage pdb, int currCid)
    {
        SaveToPersistedState(writeSet.KVWriteSet, pdb, Database.KV, currCid);
        SaveToPersistedState(writeSet.WarehouseWriteSet, pdb, Database.Warehouse, currCid);
        SaveToPersistedState(writeSet.DistrictWriteSet, pdb, Database.District, currCid);
        SaveToPersistedState(writeSet.CustomerWriteSet, pdb, Database.Customer, currCid);
        SaveToPersistedState(writeSet.ItemWriteSet, pdb, Database.Item, currCid);
        SaveToPersistedState(writeSet.StockWriteSet, pdb, Database.Stock, currCid);
        SaveToPersistedState(writeSet.HistoryWriteSet, pdb, Database.History, currCid);
        SaveToPersistedState(writeSet.NewOrderWriteSet, pdb, Database.NewOrder, currCid);
        SaveToPersistedState(writeSet.OrderWriteSet, pdb, Database.Order, currCid);
        SaveToPersistedState(writeSet.OrderLineWriteSet, pdb, Database.OrderLine, currCid);

        ConstructCustomerIndexByLastName(writeSet.CustomerWriteSet, pdb.CustomerIndexByLastName);
    }

    private static void SaveToPersistedState<TKey, TValue>(IDictionary<TKey, TValue> writeSet,
        PersistentStorage persistedDB,
        Database database,
        int currCid)
    {

        lock (TempStorageLocks[database])
        {
            foreach (var (key, value) in writeSet)
            {

                if (persistedDB.Get(database, key, out PersistEntry<TValue> existingEntry))
                {
                    existingEntry.Value = value;
                    existingEntry.Cid = currCid;
                }
                else
                {
                    var val = new PersistEntry<TValue>(value, currCid);
                    persistedDB.Set(database, key, val);
                }
            }
        }
    }

    private static void ConstructCustomerIndexByLastName(IDictionary<(long CWID, long CDID, long CID), Customer> customerWriterSet, Dictionary<(long, long, string), HashSet<(long CWID, long CDID, long CID)>> customerIndexByLastName)
    {
        foreach (var (key, customer) in customerWriterSet)
        {
            var customerKey = (customer.C_W_ID, customer.C_D_ID, customer.C_LAST);

            if (!customerIndexByLastName.TryGetValue(customerKey, out var customerList))
            {
                customerList = [];
                customerIndexByLastName[customerKey] = customerList;
            }
            customerList.Add(key);
        }
    }






}
