using System;
using System.IO;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using Microsoft.Extensions.Logging;

namespace Minerva.DB_Server.Network;


public class ServerSender
{
    private string _serverAddr;
    private readonly int _serverPort;
    private readonly TcpClient[] _tcpClients;
    private readonly Stream[] _networkStreams;




    private CancellationTokenSource _cts = new();
    private readonly ILogger _logger = LoggerManager.GetLogger();


    public ServerSender(string serverAddress, int port)
    {
        _serverAddr = serverAddress;
        _serverPort = port + Consts.Network.SERVER_MSG_PORT_OFFSET;



        _tcpClients = new TcpClient[Enum.GetValues<ServerMessageTypes>().Length];
        _networkStreams = new Stream[Enum.GetValues<ServerMessageTypes>().Length];


        foreach (var enumValue in Enum.GetValues<ServerMessageTypes>())
        {
            _tcpClients[(int)enumValue] = new TcpClient()
            {
                NoDelay = true
            };
        }
    }

    public async Task ConnectAsync()
    {

        foreach (var t in Enum.GetValues<ServerMessageTypes>())
        {
            int retryCount = 0;
            const int maxRetries = 5;

            while (retryCount < maxRetries)
            {   
                if (!_tcpClients[(int)t].Connected)
                {
                    await _tcpClients[(int)t].ConnectAsync(_serverAddr, _serverPort, _cts.Token);
                }

                _networkStreams[(int)t] = _tcpClients[(int)t].GetStream();
                // write one byte to indicate stream type
                await _networkStreams[(int)t].WriteAsync(new byte[] { (byte)t },
                    _cts.Token);
                // wait for one byte to acknowledge stream type
                var buffer = new byte[1];
                await _networkStreams[(int)t].ReadExactlyAsync(buffer, 0, 1, _cts.Token);

                if (buffer[0] != (byte)t)
                {
                    _logger.LogError("Failed to establish stream of type {StreamType} with server {Address}:{Port} - Acknowledgement mismatch", t, _serverAddr, _serverPort);
                    _networkStreams[(int)t].Dispose();
                    retryCount++;
                    continue;
                }
                else
                {
                    break; // Exit the retry loop if successful
                }
            }

            if (retryCount == maxRetries)
            {
                throw new Exception($"Failed to establish stream of type {t} with server {_serverAddr}:{_serverPort} after {maxRetries} attempts.");
            }
        }
    }

    public async Task SendMessage<T>(T message, ServerMessageTypes streamToUse)
    {
        
        using var memoryOwner = ProtoMessageHelper.SerializeToMemoryOwner(message);
        await SendMessageBytes(memoryOwner.Memory, streamToUse);
    }


    public async Task SendMessageBytes(ReadOnlyMemory<byte> message, ServerMessageTypes streamToUse)
    {
        

        try
        {
            var stream = _networkStreams[(int)streamToUse];
            await stream.WriteAsync(message, _cts.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            _logger.LogError("Failed to send message to server {Address}:{Port} - {ex.Message} {stacktrace}", _serverAddr, _serverPort, ex.Message, ex.StackTrace);
            throw;
        }

    }







    public void Dispose()
    {
        _cts.Cancel();
        foreach (var client in _tcpClients)
        {
            client.Close();
            client.Dispose();
        }
    }
}