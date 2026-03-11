using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Minerva.DB_Server;
using Minerva.DB_Server.Interface;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server.Network;
using Minerva.DB_Server.Network.Protos;

namespace Minerva.DB_Server.Network;

public class ReceivedClientRequestHandler : IReceivedRequestHandler<ClientRequest>
{

    private ProtoClientInterface _protoClientInterface;
    private readonly Stream _stream;

    private readonly ILogger _logger = LoggerManager.GetLogger();

    public ReceivedClientRequestHandler(ProtoClientInterface protoClientInterface, Stream stream)
    {
        _protoClientInterface = protoClientInterface;
        _stream = stream;
    }


    public async void HandleMessage(ClientRequest message)
    {
        TxResult result;

        ClientRequest msg = message as ClientRequest;

        try
        {
            result = await _protoClientInterface.NewQuery(msg);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process client request {q}", msg);
            TxResult txResult = new()
            {
                SeqId = msg.SeqId,
                Executed = false,
                TxResultStr = $"Error processing request: {ex.Message}"
            };

            result = txResult;
        }

        if (result != null)
        {
            try
            {

                //Serializer.SerializeWithLengthPrefix(stream, result, PrefixStyle.Fixed32BigEndian);
                using var memoryOwner = ProtoMessageHelper.SerializeToMemoryOwner(result);
                await _stream.WriteAsync(memoryOwner.Memory);

            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
        }
    }
}

public class ReceivedServerBatchHandler(LogReceiveRequestHandler logReqHandler) : IReceivedRequestHandler<Batch>
{
    private LogReceiveRequestHandler _logReqHandler = logReqHandler;

    public void HandleMessage(Batch message)
    {
        _logReqHandler.HandleReceivedBatch(message);

    }
}

public class ReceivedBatchAckHandler(LogReceiveRequestHandler logReqHandler) : IReceivedRequestHandler<BatchAckMsg>
{
    private LogReceiveRequestHandler _logReqHandler = logReqHandler;

    public void HandleMessage(BatchAckMsg message)
    {
        _logReqHandler.HandleReceivedAcknowledgeBatch(message);

    }
}

public class ReceivedPoAHandler(LogReceiveRequestHandler logReqHandler) : IReceivedRequestHandler<PoAMsg>
{
    private LogReceiveRequestHandler _logReqHandler = logReqHandler;

    public void HandleMessage(PoAMsg message)
    {
        _logReqHandler.HandleReceivedPoA(message);

    }
}

public class ReceivedBatchRequestHandler(LogReceiveRequestHandler logReqHandler) : IReceivedRequestHandler<BatchRequestMsg>
{
    private LogReceiveRequestHandler _logReqHandler = logReqHandler;

    public void HandleMessage(BatchRequestMsg message)
    {
        _logReqHandler.HandleReceivedRequestBatch(message);

    }
}

public class ReceivedConsistentCutHandler(LogReceiveRequestHandler logReqHandler) : IReceivedRequestHandler<ConsistentCutMsg>
{
    private LogReceiveRequestHandler _logReqHandler = logReqHandler;

    public void HandleMessage(ConsistentCutMsg message)
    {
        _logReqHandler.HandleReceivedCommittedIndx(message);
    }
}

public class ReceivedReplicaEpochAnnoHandler(LogReceiveRequestHandler logReqHandler) : IReceivedRequestHandler<ReplicaEpochAnnoMsg>
{
    private LogReceiveRequestHandler _logReqHandler = logReqHandler;

    public void HandleMessage(ReplicaEpochAnnoMsg message)
    {
        _logReqHandler.HandleReceivedBroadCastCurEpochId(message);
    }
}

