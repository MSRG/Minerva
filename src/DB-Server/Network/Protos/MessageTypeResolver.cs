using System;
using System.Collections.Generic;
using Minerva.DB_Server.MinervaLog;

namespace Minerva.DB_Server.Network.Protos;


public enum MessageType : byte
{
    // Need to start at 1 for protobuf resolver
    padding = 1,
    PingRequest,
    PingReply,
    ClientRequest,
    TxResult,

    BatchAckMsg,
    PoAMsg,
    BatchRequestMsg,
    ConsistentCutMsg,
    ReplicaEpochAnnoMsg,
    BatchMsg

    
}

public static class MessageTypeResolver
{
    private static readonly Dictionary<MessageType, Type> TypeMap = new()
    {
        { MessageType.PingRequest, typeof(PingRequest) },
        { MessageType.PingReply, typeof(PingReply) },

        { MessageType.ClientRequest, typeof(ClientRequest) },
        { MessageType.TxResult, typeof(TxResult) },

        { MessageType.BatchAckMsg, typeof(BatchAckMsg) },
        { MessageType.PoAMsg, typeof(PoAMsg) },
        { MessageType.BatchRequestMsg, typeof(BatchRequestMsg) },
        { MessageType.ConsistentCutMsg, typeof(ConsistentCutMsg) },
        { MessageType.ReplicaEpochAnnoMsg, typeof(ReplicaEpochAnnoMsg) },
        { MessageType.BatchMsg, typeof(Batch) }

    };

    public static Type ResolveType(int fieldNumber)
    {
        if (!TypeMap.TryGetValue((MessageType)fieldNumber, out Type type))
            throw new Exception("Invalid field number");
        return type;
    }

    public static int ResolveFieldNumber<T>()
    {
        return (byte)Enum.Parse<MessageType>(typeof(T).Name);
    }

}