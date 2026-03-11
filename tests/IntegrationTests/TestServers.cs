using Xunit;
using System;
using Minerva.DB_Server;
using System.Threading.Tasks;
using System.Threading;
using System.IO;
using Minerva.DB_Server.Network.Protos;
using System.Collections.Generic;
using Minerva.DB_server.Interface;

namespace Tests;

[Collection("CannotParallel")]
public class TestServers : IDisposable
{
    public readonly MinervaService[] Services = new MinervaService[3];
    public readonly NodeInfo[][] nodesConfigs = new NodeInfo[3][];
    public readonly MinervaConfig config;

    public TestServers()
    {
        // Get absolute path to logger config
        LoggerManager.ConfigureLogger("logger_config.json");

        // Create 3 Node and corresponding node info
        for (int i = 0; i < 3; i++)
        {
            nodesConfigs[i] = new NodeInfo[3];
            for (int j = 0; j < 3; j++)
            {
                nodesConfigs[i][j] = new NodeInfo
                {
                    Id = j,
                    Address = "127.0.0.1",
                    Port = 5000 + (j * 1000),
                    IsSelfNode = i == j
                };
            }
        }

        // Create a config
        config = new()
        {
            ReadStorage = "",
            DatabaseToLoad = [],
            SolverExact = true,
            ReplicaPriority = [0, 1, 2],
            LocalEpochInterval = 5,
            CoordinatorGlobalEpochInterval = 15,
        };


        // Create 3 services
        for (int i = 0; i < 3; i++)
        {
            int index = i; // Capture variable for closure
            Services[index] = new MinervaService(config, nodesConfigs[i]);

            _ = Task.Run(() =>
            {
                Services[index].Init();
                Services[index].StartInterface(InterfaceType.Test);
            });
        }

        Thread.Sleep(5000); // wait for servers to start

    }



    [Fact]
    public async Task TestKVTransactions()
    {
        for (int i = 0; i < 3; i++)
        {
            Assert.True(Services[i].Running, $"Service {i} is not running");
        }

        // Test 1: Serial transactions
        Console.WriteLine("Starting serial transaction test...");
        List<TxResult> serialResults = new();

        // Send 10 serial transactions
        for (uint i = 0; i < 10; i++)
        {
            var request = CreateKVRequest(i, $"key_{i}", $"value_{i}", OpType.Set);
            var result = await ((TestInterface)Services[0].Interface).NewQuery(request);
            serialResults.Add(result);

            Assert.True(result.Executed, $"Serial transaction {i} failed to execute");
            Console.WriteLine($"Serial transaction {i} completed");
        }

        // Wait for replication
        Thread.Sleep(3000);

        // Verify serial transactions are replicated correctly
        await VerifyConsistency("key_5", "value_5");
        Console.WriteLine("Serial transaction consistency verified");

        // Test 2: Concurrent transactions
        Console.WriteLine("Starting concurrent transaction test...");
        List<Task<TxResult>> concurrentTasks = new();

        // Send 30 concurrent transactions
        for (uint i = 10; i < 40; i++)
        {
            var request = CreateKVRequest(i, $"key_{i}", $"value_{i}", OpType.Set);
            // Round-robin across servers for concurrent load
            var serverIdx = (int)(i % 3);
            var task = ((TestInterface)Services[serverIdx].Interface).NewQuery(request);
            concurrentTasks.Add(task);
        }

        // Wait for all concurrent transactions to complete
        var results = await Task.WhenAll(concurrentTasks.ToArray());

        // Check all executed successfully
        foreach (var result in results)
        {
            Assert.True(result.Executed, $"Concurrent transaction {result.SeqId} failed to execute");
        }
        Console.WriteLine($"All {concurrentTasks.Count} concurrent transactions completed");

        // Wait for replication
        Thread.Sleep(3000);

        // Verify concurrent transactions are replicated correctly
        await VerifyConsistency("key_20", "value_20");
        await VerifyConsistency("key_35", "value_35");
        Console.WriteLine("Concurrent transaction consistency verified");
    }


    public ClientRequest CreateKVRequest(uint seqId, string key, string value, OpType opType)
    {
        return new ClientRequest
        {
            Type = QueryType.Ycsb,
            SeqId = seqId,
            KVCmds = new List<KV>
            {
                new KV
                {
                    Type = opType,
                    Shard = 0, // Use shard 0 for simplicity
                    Key = key,
                    Value = value
                }
            }
        };
    }

    private async Task VerifyConsistency(string key, string expectedValue)
    {
        // Query all 3 replicas for the same key and verify they return the same value
        List<string> values = new();

        for (int i = 0; i < 3; i++)
        {
            var request = CreateKVRequest((uint)(1000 + i), key, "", OpType.Get);
            var result = await ((TestInterface)Services[i].Interface).NewQuery(request);

            Assert.True(result.Executed, $"Get query failed on replica {i}");

            // The result should contain the value (may have semicolon appended by query executor)
            var returnedValue = result.TxResultStr.TrimEnd(';');
            values.Add(returnedValue);
            Console.WriteLine($"Replica {i}: Key '{key}' = '{returnedValue}'");
        }

        // Verify all replicas returned the same value
        for (int i = 0; i < values.Count; i++)
        {
            Assert.Equal(expectedValue, values[i]);
        }

        Console.WriteLine($"All replicas consistent for key '{key}'");
    }


    public void Dispose()
    {
        foreach (var service in Services)
        {
            service.StopInterface();
            service.Dispose();
        }

    }
}