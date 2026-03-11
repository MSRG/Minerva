using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace Minerva.DB_Server;

public static class Stats
{
    public static int TotalAppliedTx  = 0;
    public static int TotalLocalExecuted  = 0;
    public static int StaledTxs = 0;
    public static int ConflictedTx = 0;
    public static int NonLocalExecutedTx = 0;

    public static long Notes = 0;
    public static long Notes2 = 0;

    private static Thread? _throughputThread;
    private static volatile bool _stopReporting = false;
    private static readonly Stopwatch _stopwatch = new();
    private static int _lastTxCount = 0;
    private static long _lastElapsedTicks = 0;

    public static void StartThroughputReporting()
    {
        _stopReporting = false;
        _lastTxCount = 0;
        _lastElapsedTicks = 0;
        _stopwatch.Restart();

        _throughputThread = new Thread(ThroughputReportLoop)
        {
            IsBackground = true,
            Name = "ThroughputReporter"
        };
        _throughputThread.Start();
    }

    public static void StopThroughputReporting()
    {
        _stopReporting = true;
        _throughputThread?.Join(timeout: TimeSpan.FromSeconds(2));
        _stopwatch.Stop();
    }

    private static void ThroughputReportLoop()
    {
        while (!_stopReporting)
        {
            Thread.Sleep(1000);

            long currentTicks = _stopwatch.ElapsedTicks;
            int currentTxCount = Volatile.Read(ref TotalAppliedTx);

            int txDelta = currentTxCount - _lastTxCount;
            long ticksDelta = currentTicks - _lastElapsedTicks;
            double elapsedSeconds = (double)ticksDelta / Stopwatch.Frequency;

            double throughput = elapsedSeconds > 0 ? txDelta / elapsedSeconds : 0;

            Console.WriteLine($"[Throughput] {throughput:F2} txn/sec (Δtx: {txDelta}, Δt: {elapsedSeconds:F3}s, Total: {currentTxCount})");

            _lastTxCount = currentTxCount;
            _lastElapsedTicks = currentTicks;
        }
    }

    public static string GetStats()
    {
        StringBuilder sb = new();
        sb.AppendLine($"Total Transactions: {TotalAppliedTx}");
        sb.AppendLine($"Total Local Transactions: {TotalLocalExecuted}");
        sb.AppendLine($"Staled Transactions: {StaledTxs}");
        sb.AppendLine($"Conflicted Transactions: {ConflictedTx}");
        sb.AppendLine($"Non Local Executed Transactions: {NonLocalExecutedTx}");
        sb.AppendLine($"Notes1: {Notes}, Notes2: {Notes2}");
        return sb.ToString();
    }
}