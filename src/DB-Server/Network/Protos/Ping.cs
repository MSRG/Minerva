using System;
using System.ComponentModel;
using MemoryPack;

namespace Minerva.DB_Server.Network.Protos;

[MemoryPackable]
public partial class PingRequest
{
    public int NodeId { get; set; }
}

[MemoryPackable]
public partial class PingReply
{

    public int NodeId { get; set; }
}