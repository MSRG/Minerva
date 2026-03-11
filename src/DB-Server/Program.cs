// See https://aka.ms/new-console-template for more information
using System;
using System.Diagnostics;
using System.Threading;
using Minerva.DB_Server.Network.Protos;
using Minerva.DB_Server.Storage;



namespace Minerva.DB_Server;

class Program
{
    static void Main(string[] args)
    {

        // Process proc = Process.GetCurrentProcess();
        // long affinityMask = 0xFF; 
        // proc.ProcessorAffinity = (IntPtr)affinityMask;
        ThreadPool.SetMaxThreads(1000, 1000);


        string MinervaConfigPath, NodeConfigPath, LoggerConfig;
        try
        {
            MinervaConfigPath = args[0];
            NodeConfigPath = args[1];
            LoggerConfig = args[2];
        } catch (Exception)
        {
            Console.WriteLine("Usage: DB-Server <MinervaConfigPath> <NodeConfigPath> <LoggerConfigPath>");
            return;
        }


        LoggerManager.ConfigureLogger(LoggerConfig);
        MinervaConfig config = MinervaConfig.ParseConfigJson(MinervaConfigPath);
        NodeInfo[] nodes = NodeInfo.ParseConfigJson(NodeConfigPath);

        MinervaService service = new(config, nodes);
        service.Init();


        Console.CancelKeyPress += delegate(object sender, ConsoleCancelEventArgs e)
        {
            service.StopInterface();
        };


        service.StartInterface(InterfaceType.TCP);
        // whenever the interface stops
        service.Dispose();
    }
}