using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Minerva.DB_Server.Network;

namespace Minerva.DB_Server.ConflictResolver;

public sealed class ConflictGraphTrackers : IDisposable
{
	private const int DefaultEstimate = 512;

	public UnsafeConflictTracker YCSBWriteTracker { get; }
	public UnsafeConflictTracker WarehouseWriteTracker { get; }
	public UnsafeConflictTracker DistrictWriteTracker { get; }
	public UnsafeConflictTracker CustomerWriteTracker { get; }
	public UnsafeConflictTracker ItemWriteTracker { get; }
	public UnsafeConflictTracker StockWriteTracker { get; }
	public UnsafeConflictTracker HistoryWriteTracker { get; }
	public UnsafeConflictTracker NewOrderWriteTracker { get; }
	public UnsafeConflictTracker OrderWriteTracker { get; }
	public UnsafeConflictTracker OrderLineWriteTracker { get; }

	public UnsafeConflictTracker YCSBReadTrackers { get; }
	public UnsafeConflictTracker WarehouseReadTrackers { get; }
	public UnsafeConflictTracker DistrictReadTrackers { get; }
	public UnsafeConflictTracker CustomerReadTrackers { get; }
	public UnsafeConflictTracker ItemReadTrackers { get; }
	public UnsafeConflictTracker StockReadTrackers { get; }
	public UnsafeConflictTracker HistoryReadTrackers { get; }
	public UnsafeConflictTracker NewOrderReadTrackers { get; }
	public UnsafeConflictTracker OrderReadTrackers { get; }
	public UnsafeConflictTracker OrderLineReadTrackers { get; }

	public ConflictGraphTrackers(int estimatedChainCount)
	{
		int estimatedBuckets = Math.Max(DefaultEstimate, NextPow2(estimatedChainCount * 8));

		YCSBWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		WarehouseWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		DistrictWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		CustomerWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		ItemWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		StockWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		HistoryWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		NewOrderWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		OrderWriteTracker = new UnsafeConflictTracker(estimatedBuckets);
		OrderLineWriteTracker = new UnsafeConflictTracker(estimatedBuckets);

		YCSBReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		WarehouseReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		DistrictReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		CustomerReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		ItemReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		StockReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		HistoryReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		NewOrderReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		OrderReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
		OrderLineReadTrackers = new UnsafeConflictTracker(estimatedBuckets);
	}

	public void AddRWSet(TransactionsChain txc)
	{
		foreach (var tx in txc.Records)
		{
			var writeSet = tx.WriteSet;
			var readSet = tx.ReadSet;
			int rid = txc.SourceReplicaId;
			int txcIdx = txc.SolverIndex;

			// YCSB keys contain strings, so we hash them one at a time (no SIMD benefit)
			foreach (var (shard, key) in writeSet.KVWriteSet.Keys)
			{
				YCSBWriteTracker.Add(KeyHasher.Ycsb(shard, key), rid, txcIdx);
			}
			foreach (var (shard, key) in readSet.KVReadKeys)
			{
				YCSBReadTrackers.Add(KeyHasher.Ycsb(shard, key), rid, txcIdx);
			}

			// Single-component keys: Warehouse, Item
			ProcessSingleKeySet(writeSet.WarehouseWriteSet.keys, WarehouseWriteTracker, KeyHasher.WarehouseSeed, rid, txcIdx);
			ProcessSingleKeySet(readSet.WarehouseReadKeys, WarehouseReadTrackers, KeyHasher.WarehouseSeed, rid, txcIdx);
			ProcessSingleKeySet(writeSet.ItemWriteSet.keys, ItemWriteTracker, KeyHasher.ItemSeed, rid, txcIdx);
			ProcessSingleKeySet(readSet.ItemReadKeys, ItemReadTrackers, KeyHasher.ItemSeed, rid, txcIdx);

			// Two-component keys: District, Stock, History
			ProcessTwoKeySet(writeSet.DistrictWriteSet.keys, DistrictWriteTracker, KeyHasher.DistrictSeed, rid, txcIdx);
			ProcessTwoKeySet(readSet.DistrictReadKeys, DistrictReadTrackers, KeyHasher.DistrictSeed, rid, txcIdx);
			ProcessTwoKeySet(writeSet.StockWriteSet.keys, StockWriteTracker, KeyHasher.StockSeed, rid, txcIdx);
			ProcessTwoKeySet(readSet.StockReadKeys, StockReadTrackers, KeyHasher.StockSeed, rid, txcIdx);
			ProcessTwoKeySet(writeSet.HistoryWriteSet.keys, HistoryWriteTracker, KeyHasher.HistorySeed, rid, txcIdx);
			ProcessTwoKeySet(readSet.HistoryReadKeys, HistoryReadTrackers, KeyHasher.HistorySeed, rid, txcIdx);

			// Three-component keys: Customer, NewOrder, Order
			ProcessThreeKeySet(writeSet.CustomerWriteSet.keys, CustomerWriteTracker, KeyHasher.CustomerSeed, rid, txcIdx);
			ProcessThreeKeySet(readSet.CustomerReadKeys, CustomerReadTrackers, KeyHasher.CustomerSeed, rid, txcIdx);
			ProcessThreeKeySet(writeSet.NewOrderWriteSet.keys, NewOrderWriteTracker, KeyHasher.NewOrderSeed, rid, txcIdx);
			ProcessThreeKeySet(readSet.NewOrderReadKeys, NewOrderReadTrackers, KeyHasher.NewOrderSeed, rid, txcIdx);
			ProcessThreeKeySet(writeSet.OrderWriteSet.keys, OrderWriteTracker, KeyHasher.OrderSeed, rid, txcIdx);
			ProcessThreeKeySet(readSet.OrderReadKeys, OrderReadTrackers, KeyHasher.OrderSeed, rid, txcIdx);

			// Four-component keys: OrderLine
			ProcessFourKeySet(writeSet.OrderLineWriteSet.keys, OrderLineWriteTracker, KeyHasher.OrderLineSeed, rid, txcIdx);
			ProcessFourKeySet(readSet.OrderLineReadKeys, OrderLineReadTrackers, KeyHasher.OrderLineSeed, rid, txcIdx);
		}
	}

	private static void ProcessSingleKeySet(
		List<long> keys,
		UnsafeConflictTracker tracker,
		Vector128<byte> seed,
		int rid,
		int txcIdx)
	{
		int count = keys.Count;
		if (count == 0) return;

        var keySpan = CollectionsMarshal.AsSpan(keys);
		ulong[] hashArray = ArrayPool<ulong>.Shared.Rent(count);

		try
		{
			SimdKeyHasher.Hash1ComponentBatch(keySpan, hashArray.AsSpan(0, count), seed);
			tracker.AddBatch(hashArray.AsSpan(0, count), rid, txcIdx);
		}
		finally
		{
			ArrayPool<ulong>.Shared.Return(hashArray);
		}
	}

	private static void ProcessTwoKeySet(
		List<(long, long)> keys,
		UnsafeConflictTracker tracker,
		Vector128<byte> seed,
		int rid,
		int txcIdx)
	{
		int count = keys.Count;
		if (count == 0) return;

		var keySpan = CollectionsMarshal.AsSpan(keys);
		ulong[] hashArray = ArrayPool<ulong>.Shared.Rent(count);

		try
		{
			SimdKeyHasher.Hash2ComponentBatch(keySpan, hashArray.AsSpan(0, count), seed);
			tracker.AddBatch(hashArray.AsSpan(0, count), rid, txcIdx);
		}
		finally
		{
			ArrayPool<ulong>.Shared.Return(hashArray);
		}
	}

	private static void ProcessThreeKeySet(
		List<(long, long, long)> keys,
		UnsafeConflictTracker tracker,
		Vector128<byte> seed,
		int rid,
		int txcIdx)
	{
		int count = keys.Count;
		if (count == 0) return;

		var keySpan = CollectionsMarshal.AsSpan(keys);
		ulong[] hashArray = ArrayPool<ulong>.Shared.Rent(count);

		try
		{
			SimdKeyHasher.Hash3ComponentBatch(keySpan, hashArray.AsSpan(0, count), seed);
			tracker.AddBatch(hashArray.AsSpan(0, count), rid, txcIdx);
		}
		finally
		{
			ArrayPool<ulong>.Shared.Return(hashArray);
		}
	}

	private static void ProcessFourKeySet(
		List<(long, long, long, long)> keys,
		UnsafeConflictTracker tracker,
		Vector128<byte> seed,
		int rid,
		int txcIdx)
	{
		int count = keys.Count;
		if (count == 0) return;

		var keySpan = CollectionsMarshal.AsSpan(keys);
		ulong[] hashArray = ArrayPool<ulong>.Shared.Rent(count);

		try
		{
			SimdKeyHasher.Hash4ComponentBatch(keySpan, hashArray.AsSpan(0, count), seed);
			tracker.AddBatch(hashArray.AsSpan(0, count), rid, txcIdx);
		}
		finally
		{
			ArrayPool<ulong>.Shared.Return(hashArray);
		}
	}

	public void Dispose()
	{
		YCSBWriteTracker.Dispose();
		WarehouseWriteTracker.Dispose();
		DistrictWriteTracker.Dispose();
		CustomerWriteTracker.Dispose();
		ItemWriteTracker.Dispose();
		StockWriteTracker.Dispose();
		HistoryWriteTracker.Dispose();
		NewOrderWriteTracker.Dispose();
		OrderWriteTracker.Dispose();
		OrderLineWriteTracker.Dispose();

		YCSBReadTrackers.Dispose();
		WarehouseReadTrackers.Dispose();
		DistrictReadTrackers.Dispose();
		CustomerReadTrackers.Dispose();
		ItemReadTrackers.Dispose();
		StockReadTrackers.Dispose();
		HistoryReadTrackers.Dispose();
		NewOrderReadTrackers.Dispose();
		OrderReadTrackers.Dispose();
		OrderLineReadTrackers.Dispose();
	}

	private static int NextPow2(int value)
	{
		value--;
		value |= value >> 1;
		value |= value >> 2;
		value |= value >> 4;
		value |= value >> 8;
		value |= value >> 16;
		return value + 1;
	}
}


