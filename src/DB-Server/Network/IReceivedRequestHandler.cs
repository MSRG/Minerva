using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.Network;

public interface IReceivedRequestHandler1
{

    public void HandleReceivedBatch(Batch batch);
    public void HandleReceivedAcknowledgeBatch(BatchAckMsg ack);
    public void HandleReceivedPoA(PoAMsg poa);
    public Batch HandleReceivedRequestBatch(BatchRequestMsg request);
    public void HandleReceivedNoRaftGlobalCommit(ConsistentCutMsg globalCommit);
    public void HandleReceivedBroadCastCurEpochId(ReplicaEpochAnnoMsg globalCommit);
}


public interface IReceivedRequestHandler<T>
{
    public void HandleMessage(T message);
}