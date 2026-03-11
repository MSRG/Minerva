using MemoryPack;
using System.Collections.Generic;

namespace Minerva.DB_Server.Network.Protos;


[MemoryPackable]
public partial class WriteSet
{
    // YCSB
    public ListMap<(int shard, string key), string> KVWriteSet { get; set; }

    // TPC-C
    public ListMap<long, Warehouse> WarehouseWriteSet { get; set; }

    public ListMap<(long DWID, long DID), District> DistrictWriteSet { get; set; }

    public ListMap<(long CWID, long CDID, long CID), Customer> CustomerWriteSet { get; set; }

    public ListMap<long, Item> ItemWriteSet { get; set; }

    public ListMap<(long SWID, long SIID), Stock> StockWriteSet { get; set; }

    public ListMap<(long HCID, long HDATE), History> HistoryWriteSet { get; set; }

    public ListMap<(long NOWID, long NODID, long NOOID), NewOrder> NewOrderWriteSet { get; set; }

    public ListMap<(long OWID, long ODID, long OID), Order> OrderWriteSet { get; set; }

    public ListMap<(long OLWID, long OLDID, long OLOID, long OLNUMBER), OrderLine> OrderLineWriteSet { get; set; }
    
    public WriteSet()
    {
        KVWriteSet = [];
        WarehouseWriteSet = [];
        DistrictWriteSet = [];
        CustomerWriteSet = [];
        ItemWriteSet = [];
        StockWriteSet = [];
        HistoryWriteSet = [];
        NewOrderWriteSet = [];
        OrderWriteSet = [];
        OrderLineWriteSet = [];
    }


    


}

[MemoryPackable]
public partial class ReadSet
{
    // YCSB
    public List<(int shard, string key)> KVReadKeys { get; set; }

    // TPC-C
    public List<long> WarehouseReadKeys { get; set; }

    public List<(long DWID, long DID)> DistrictReadKeys { get; set; }

    public List<(long CWID, long CDID, long CID)> CustomerReadKeys { get; set; }

    public List<long> ItemReadKeys { get; set; }

    public List<(long SWID, long SIID)> StockReadKeys { get; set; }

    public List<(long HCID, long HDATE)> HistoryReadKeys { get; set; }

    public List<(long NOWID, long NODID, long NOOID)> NewOrderReadKeys { get; set; }

    public List<(long OWID, long ODID, long OID)> OrderReadKeys { get; set; }

    public List<(long OLWID, long OLDID, long OLOID, long OLNUMBER)> OrderLineReadKeys { get; set; }
    

    public ReadSet()
    {
        KVReadKeys = [];
        WarehouseReadKeys = [];
        DistrictReadKeys = [];
        CustomerReadKeys = [];
        ItemReadKeys = [];
        StockReadKeys = [];
        HistoryReadKeys = [];
        NewOrderReadKeys = [];
        OrderReadKeys = [];
        OrderLineReadKeys = [];
    }
    

}

[MemoryPackable]
public partial class KeyAccessedFromSnapshotStore
{
    // YCSB
    public Dictionary<(int shard, string key), int> KVKeyAccessedFromSnapshot { get; set; }

    // TPC-C
    public Dictionary<long, int> WarehouseReadsFromSnapshot { get; set; }

    public Dictionary<(long DWID, long DID), int> DistrictReadsFromSnapshot { get; set; }

    public Dictionary<(long CWID, long CDID, long CID), int> CustomerReadsFromSnapshot { get; set; }

    public Dictionary<long, int> ItemReadsFromSnapshot { get; set; }

    public Dictionary<(long SWID, long SIID), int> StockReadsFromSnapshot { get; set; }

    public Dictionary<(long HCID, long HDATE), int> HistoryReadsFromSnapshot { get; set; }

    public Dictionary<(long NOWID, long NODID, long NOOID), int> NewOrderReadsFromSnapshot { get; set; }

    public Dictionary<(long OWID, long ODID, long OID), int> OrderReadsFromSnapshot { get; set; }

    public Dictionary<(long OLWID, long OLDID, long OLOID, long OLNUMBER), int> OrderLineReadsFromSnapshot { get; set; }
    
    public KeyAccessedFromSnapshotStore()
    {
        KVKeyAccessedFromSnapshot = [];
        WarehouseReadsFromSnapshot = [];
        DistrictReadsFromSnapshot = [];
        CustomerReadsFromSnapshot = [];
        ItemReadsFromSnapshot = [];
        StockReadsFromSnapshot = [];
        HistoryReadsFromSnapshot = [];
        NewOrderReadsFromSnapshot = [];
        OrderReadsFromSnapshot = [];
        OrderLineReadsFromSnapshot = [];
    }
}