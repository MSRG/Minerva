using System;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.Net;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using System.IO;
using Microsoft.Extensions.Logging.Abstractions;
using System.IO.Pipelines;
using System.Buffers;
using Minerva.DB_Server.Network.Protos;
using System.Diagnostics;
using Minerva.DB_Server.MinervaLog;
using Minerva.DB_Server.Interface;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Minerva.DB_Server.Network;


public class Server
{
    private TcpListener _clientListener;
    private TcpListener _serverListener;

    private readonly CancellationTokenSource _cts = new();
    private readonly LogReceiveRequestHandler _logReqHandler;
    private readonly ProtoClientInterface _protoClientInterface;
    private readonly ILogger _logger = LoggerManager.GetLogger();


    public Server(int port, LogReceiveRequestHandler logReqHandler, ProtoClientInterface protoClientInterface)
    {
        _clientListener = new TcpListener(IPAddress.Any, port);
        _serverListener = new TcpListener(IPAddress.Any, port + Consts.Network.SERVER_MSG_PORT_OFFSET);

        _logReqHandler = logReqHandler;
        _protoClientInterface = protoClientInterface;
    }

    private delegate Task MessageHandlerDelegate(TcpClient client, CancellationToken ct);
    public void Start()
    {
        RunListeners(_serverListener, HandleServerMessageAsync, _cts.Token);
        RunListeners(_clientListener, HandleClientMessageAsync, _cts.Token);    
    }

    private void RunListeners(TcpListener listener, MessageHandlerDelegate handler, CancellationToken ct)
    {
        listener.Start();

        Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var tcpClient = await listener.AcceptTcpClientAsync();
                    tcpClient.NoDelay = true;

                    // Handle each client connection concurrently
                    _ = Task.Run(async () => await handler(tcpClient, ct)).ContinueWith(t =>
                            {
                                if (t.IsFaulted)
                                {
                                    _logger.LogError($"Error handling client: {t.Exception?.GetBaseException().Message}");
                                }
                            }, TaskContinuationOptions.OnlyOnFaulted);
                }
                catch (ObjectDisposedException)
                {
                    // Server is shutting down
                    break;
                }
                catch (OperationCanceledException)
                {
                    // Server is shutting down
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError("Error accepting client: {ex.Message}", ex.Message);
                    throw;
                }
            }
        });
    }

    private async Task HandleServerMessageAsync(TcpClient client, CancellationToken ct)
    {
        using (client)
        {
            var stream = client.GetStream();
            //var temp = client.GetStream();
            //var stream = new MonitoredNetworkStream(temp, BandwidthMonitorGlobal.Instance);


            // read one byte to determine message type      
            var messageType = Enum.Parse<ServerMessageTypes>(stream.ReadByte().ToString());
            // reply one byte to acknowledge receipt of message type

            
            await stream.WriteAsync(new byte[] { (byte)messageType }, ct);
            try
            {
                switch (messageType)
                {
                    case ServerMessageTypes.BatchAckMsg:

                        var mpBatchAck = new MessageProcessorGeneric<BatchAckMsg>(stream, new ReceivedBatchAckHandler(_logReqHandler));
                        await mpBatchAck.ProcessMessagesAsync(ct);
                        await mpBatchAck.CompleteAsync();
                        break;
                    case ServerMessageTypes.PoAMsg:
                        var mpPoA = new MessageProcessorGeneric<PoAMsg>(stream, new ReceivedPoAHandler(_logReqHandler));
                        await mpPoA.ProcessMessagesAsync(ct);
                        await mpPoA.CompleteAsync();
                        break;
                    case ServerMessageTypes.BatchRequestMsg:
                        var mpBatchRequest = new MessageProcessorGeneric<BatchRequestMsg>(stream, new ReceivedBatchRequestHandler(_logReqHandler));
                        await mpBatchRequest.ProcessMessagesAsync(ct);
                        await mpBatchRequest.CompleteAsync();
                        break;
                    case ServerMessageTypes.ConsistentCutMsg:
                        var mpConsistentCut = new MessageProcessorGeneric<ConsistentCutMsg>(stream, new ReceivedConsistentCutHandler(_logReqHandler));
                        await mpConsistentCut.ProcessMessagesAsync(ct);
                        await mpConsistentCut.CompleteAsync();
                        break;
                    case ServerMessageTypes.ReplicaEpochAnnoMsg:
                        var mpReplicaEpochAnno = new MessageProcessorGeneric<ReplicaEpochAnnoMsg>(stream, new ReceivedReplicaEpochAnnoHandler(_logReqHandler));
                        await mpReplicaEpochAnno.ProcessMessagesAsync(ct);
                        await mpReplicaEpochAnno.CompleteAsync();
                        break;
                    case ServerMessageTypes.BatchMsg:
                        var mpBatch = new MessageProcessorGeneric<Batch>(stream, new ReceivedServerBatchHandler(_logReqHandler));
                        await mpBatch.ProcessMessagesAsync(ct);
                        await mpBatch.CompleteAsync();
                        break;
                    default:
                        _logger.LogWarning("Unknown message type received: {messageType}", messageType);
                        return;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error handling client of {Address}:{Port} - {ex.Message} {stackTrace}", ((IPEndPoint)client.Client.RemoteEndPoint).Address, ((IPEndPoint)client.Client.RemoteEndPoint).Port, ex.Message, ex.StackTrace);
                throw;
            }
        }
    }

    private async Task HandleClientMessageAsync(TcpClient client, CancellationToken ct)
    {
        using (client)
        {
            var stream = client.GetStream();
            // var temp = client.GetStream();
            // var stream = new MonitoredNetworkStream(temp, BandwidthMonitorGlobal.Instance);

            MessageProcessorGeneric<ClientRequest> messageProcessor = new(stream, new ReceivedClientRequestHandler(_protoClientInterface, stream));

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    await messageProcessor.ProcessMessagesAsync(ct);
                }

            }
            catch (Exception ex)
            {
                _logger.LogError("Error handling client of {Address}:{Port} - {ex.Message} {stackTrace}", ((IPEndPoint)client.Client.RemoteEndPoint).Address, ((IPEndPoint)client.Client.RemoteEndPoint).Port, ex.Message, ex.StackTrace);
                throw;
            }
        }
    }


    public void Stop()
    {
        _cts.Cancel();
        _serverListener.Stop();
        _clientListener.Stop();
    }
}


