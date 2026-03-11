using MemoryPack;
using System.Collections.Generic;

namespace Minerva.DB_Server.Network.Protos;


[MemoryPackable]
public partial class BatchAckMsg
{
    public int SourceReplicaId { get; set; }

    public int BatchId { get; set; }

    public int FromId { get; set; }
}

[MemoryPackable]
public partial class PoAMsg
{
    public int SourceReplicaId { get; set; }

    public int BatchId { get; set; }
}

[MemoryPackable]
public partial class BatchRequestMsg
{
    public int SourceReplicaId { get; set; }
    
    public int BatchId { get; set; }
    
    public int FromId { get; set; }
}

[MemoryPackable]
public partial class ConsistentCutMsg
{
    public List<int> ConsistentCutIndices { get; set; }
    
    public int EpochId { get; set; }
}

[MemoryPackable]
public partial class ReplicaEpochAnnoMsg
{
    public int ReplicaId { get; set; }
    
    public int EpochId { get; set; }
}

