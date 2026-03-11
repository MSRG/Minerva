using System;
using System.Collections.Generic;
using MemoryPack;

namespace Minerva.DB_Server.Network.Protos;

public enum QueryType
{
    Stop = 1,
    Stats = 2,
    SaveState = 3,
    Ycsb = 4,
    Tpccno = 5, // new order
    Tpccos = 6, // order status
    Tpccp = 7, // payment
    Tpccsl = 8, // stock level
    Tpccd = 9, // delivery
    Tpccli = 10, // load item
    Tpcclw = 11, // load warehouse
    Tpccld = 12, // load district
    Tpcclc = 13, // load customer
    Tpccls = 14, // load stock
    Tpcclio = 15, // load initial orders
    Tpcclh = 16, // load history
}

public enum OpType
{
    Set = 1,
    Get = 2,
    Delete = 3
}

[MemoryPackable]
public partial class TxResult
{   
    public uint SeqId { get; set; } 
    public bool Executed { get; set; }
    public string TxResultStr { get; set; }
}

[MemoryPackable]
public partial class ClientRequest
{
    public QueryType Type { get; set; }
    
    public uint SeqId { get; set; } 
    
    // YCSB
    public List<KV> KVCmds { get; set; }

    // TPC-C
    // query
    public TPCCNO Tpccno { get; set; }
    
    public TPCCP Tpccp { get; set; }

    // load
    public TPCCLI Tpccli { get; set; }
    
    public TPCCLW Tpcclw { get; set; }
    
    public TPCCLD Tpccld { get; set; }
    
    public TPCCLC Tpcclc { get; set; }
    
    public TPCCLS Tpccls { get; set; }
    
    public TPCCLIO Tpcclio { get; set; }
    
    public TPCCLH Tpcclh { get; set; }
}

[MemoryPackable]
public partial class KV
{
    public OpType Type { get; set; }

    public int Shard { get; set; }

    public string Key { get; set; }
    
    public string Value { get; set; }
}

[MemoryPackable]
public partial class TPCCNO
{
    public int W_ID { get; set; }
    
    public int D_ID { get; set; }
    
    public int C_ID { get; set; }
    
    public int O_ID { get; set; }
    
    public List<NOItem> Items { get; set; }
}

[MemoryPackable]
public partial class NOItem
{
    public int I_ID { get; set; }
    
    public int W_ID { get; set; }
    
    public int Q { get; set; }
}

[MemoryPackable]
public partial class TPCCP
{
    public int WID { get; set; }
    
    public int DID { get; set; }
    
    public int CID { get; set; } // Customer ID (optional)
    
    public string CLAST { get; set; } // Customer Last Name (optional)
    
    public long AMOUNT { get; set; } // Payment Amount
}

[MemoryPackable]
public partial class TPCCLI
{
    public int I { get; set; }
}

[MemoryPackable]
public partial class TPCCLW
{
    public int W { get; set; }
}

[MemoryPackable]
public partial class TPCCLD
{
    public int D { get; set; }
    
    public int WarehouseId { get; set; }
}

[MemoryPackable]
public partial class TPCCLC
{
    public int WarehouseId { get; set; }
    
    public int DistrictID { get; set; }
    
    public int C { get; set; }
}

[MemoryPackable]
public partial class TPCCLS
{
    public int I { get; set; }
    
    public int WarehouseId { get; set; }
}

[MemoryPackable]
public partial class TPCCLIO
{
    public int D { get; set; }
    
    public int W { get; set; }
    
    public int O { get; set; }
}

[MemoryPackable]
public partial class TPCCLH
{
    public int D { get; set; }
    
    public int W { get; set; }
    
    public int C { get; set; }
}