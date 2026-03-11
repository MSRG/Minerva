using MemoryPack;
using System.Collections.Generic;

namespace Minerva.DB_Server.Network.Protos;

public enum GrpcBatchStatus
{
    Active = 0,
    LocalCompleted = 1,
    Available = 2,
    PoASent = 3,
    Committed = 4
}


public enum TxConflict
{
    Conflict = 0,
    None = 1,
    Stale = 2,
    NonExecuted = 3
}

[MemoryPackable]
public partial class TransactionRecord
{
    public int Tid { get; set; }

    public List<int> PrevTids { get; set; }

    public ClientRequest Query { get; set; }

    public WriteSet WriteSet { get; set; }

    public ReadSet ReadSet { get; set; }

    public KeyAccessedFromSnapshotStore KeyAccessedFromSnapshot { get; set; }

    // default to conflict, because ConflictGraphSolver returns only the non conflict ones.
    public TxConflict ConflictStatus = TxConflict.Conflict;
    // used for determinstic re-execution
    public int SourceReplicaId;

    
    public TransactionRecord()
    {
        PrevTids = [];
    }

}

// [MemoryPackable]
// public partial class BatchMsg
// {
//     public GrpcBatchStatus Status { get; set; }
    
//     public int BatchId { get; set; }
    
//     public int SourceReplicaId { get; set; }
    
//     public List<TransactionRecord> Transactions { get; set; }
// }