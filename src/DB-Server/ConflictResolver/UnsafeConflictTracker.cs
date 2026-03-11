using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Minerva.DB_Server.Network;

namespace Minerva.DB_Server.ConflictResolver;


public sealed unsafe class UnsafeConflictTracker : IDisposable
{
	private const float LoadFactor = 0.75f;
	private const int DefaultCapacity = 64;
	private const int InitialEntryCapacity = 4;

	private Bucket* _buckets;
	private int _bucketCapacity;
	private int _bucketCount;
	private int _resizeThreshold;
	private bool _disposed;

	public UnsafeConflictTracker(int estimatedCapacity)
	{
		if (estimatedCapacity <= 0)
		{
			estimatedCapacity = DefaultCapacity;
		}

		Initialize(estimatedCapacity);
	}

	public void Add(ulong keyHash, int rid, int txcIdx)
	{
		if (_disposed)
		{
			throw new ObjectDisposedException(nameof(UnsafeConflictTracker));
		}

		ref var bucket = ref LocateBucket(keyHash);
		bool isNew = bucket.Count == 0 && bucket.Capacity == 0 && bucket.Entries == null;

		AppendEntry(ref bucket, rid, txcIdx);

		if (isNew)
		{
			_bucketCount++;

			if (_bucketCount >= _resizeThreshold)
			{
				Resize();
			}
		}
	}

	/// <summary>
	/// Add multiple pre-hashed keys in batch. More efficient than calling Add repeatedly.
	/// </summary>
	public void AddBatch(ReadOnlySpan<ulong> keyHashes, int rid, int txcIdx)
	{
		if (_disposed)
		{
			throw new ObjectDisposedException(nameof(UnsafeConflictTracker));
		}

		if (keyHashes.IsEmpty)
		{
			return;
		}

		// Pre-check if we need to resize to avoid multiple resizes during batch
		int potentialNewBuckets = keyHashes.Length;
		while (_bucketCount + potentialNewBuckets >= _resizeThreshold)
		{
			Resize();
		}

		for (int i = 0; i < keyHashes.Length; i++)
		{
			ulong keyHash = keyHashes[i];
			ref var bucket = ref LocateBucket(keyHash);
			bool isNew = bucket.Count == 0 && bucket.Capacity == 0 && bucket.Entries == null;

			AppendEntry(ref bucket, rid, txcIdx);

			if (isNew)
			{
				_bucketCount++;
			}
		}
	}

	public BucketEnumerator GetEnumerator() => new BucketEnumerator(_buckets, _bucketCapacity);

	public bool TryGetBucket(ulong key, out BucketView view)
	{
		if (_bucketCapacity == 0)
		{
			view = default;
			return false;
		}

		int mask = _bucketCapacity - 1;
		int index = (int)(key & (ulong)mask);
		int scanned = 0;

		while (scanned < _bucketCapacity)
		{
			Bucket* bucketPtr = _buckets + index;

			if (bucketPtr->Entries == null && bucketPtr->Capacity == 0)
			{
				break;
			}

			if (bucketPtr->Key == key && bucketPtr->Count > 0)
			{
				view = new BucketView(bucketPtr);
				return true;
			}

			index = (index + 1) & mask;
			scanned++;
		}

		view = default;
		return false;
	}

	public void Dispose()
	{
		Dispose(true);
		GC.SuppressFinalize(this);
	}

	~UnsafeConflictTracker()
	{
		Dispose(false);
	}

	private void Initialize(int estimatedCapacity)
	{
		_bucketCapacity = NextPowerOfTwo(Math.Max(DefaultCapacity, estimatedCapacity));
		_buckets = (Bucket*)NativeMemory.AllocZeroed((nuint)_bucketCapacity, (nuint)sizeof(Bucket));
		_bucketCount = 0;
		_resizeThreshold = (int)(_bucketCapacity * LoadFactor);
	}

	private ref Bucket LocateBucket(ulong keyHash)
	{
		if (_bucketCapacity == 0)
		{
			Initialize(DefaultCapacity);
		}

		int mask = _bucketCapacity - 1;
		int index = (int)(keyHash & (ulong)mask);

		while (true)
		{
			ref var bucket = ref GetBucketRef(index);

			if (bucket.Entries == null && bucket.Capacity == 0)
			{
				bucket.Key = keyHash;
				return ref bucket;
			}

			if (bucket.Key == keyHash)
			{
				return ref bucket;
			}

			index = (index + 1) & mask;
		}
	}

	private void Resize()
	{
		int newCapacity = _bucketCapacity << 1;
		Bucket* newBuckets = (Bucket*)NativeMemory.AllocZeroed((nuint)newCapacity, (nuint)sizeof(Bucket));

		for (int i = 0; i < _bucketCapacity; i++)
		{
			ref var bucket = ref GetBucketRef(i);
			if (bucket.Entries == null)
			{
				continue;
			}

			int mask = newCapacity - 1;
			int index = (int)(bucket.Key & (ulong)mask);

			while (true)
			{
				ref var target = ref Unsafe.Add(ref Unsafe.AsRef<Bucket>(newBuckets), index);

				if (target.Entries == null && target.Capacity == 0)
				{
					target = bucket;
					break;
				}

				index = (index + 1) & mask;
			}
		}

		NativeMemory.Free(_buckets);
		_buckets = newBuckets;
		_bucketCapacity = newCapacity;
		_resizeThreshold = (int)(_bucketCapacity * LoadFactor);
	}

	private static void AppendEntry(ref Bucket bucket, int rid, int txcIdx)
	{
		if (bucket.Capacity == 0)
		{
			bucket.Capacity = InitialEntryCapacity;
			bucket.Entries = (Entry*)NativeMemory.Alloc((nuint)bucket.Capacity, (nuint)sizeof(Entry));
		}
		else if (bucket.Count == bucket.Capacity)
		{
			int newCapacity = bucket.Capacity << 1;
			bucket.Entries = (Entry*)NativeMemory.Realloc(bucket.Entries, (nuint)newCapacity * (nuint)sizeof(Entry));
			bucket.Capacity = newCapacity;
		}

		bucket.Entries[bucket.Count].Rid = rid;
		bucket.Entries[bucket.Count].TxcIdx = txcIdx;
		bucket.Count++;
	}

	private void Dispose(bool disposing)
	{
		if (_disposed)
		{
			return;
		}

		if (_buckets != null)
		{
			for (int i = 0; i < _bucketCapacity; i++)
			{
				Bucket* bucket = _buckets + i;
				if (bucket->Entries != null)
				{
					NativeMemory.Free(bucket->Entries);
					bucket->Entries = null;
				}
			}

			NativeMemory.Free(_buckets);
			_buckets = null;
		}

		_bucketCapacity = 0;
		_bucketCount = 0;
		_resizeThreshold = 0;
		_disposed = true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ref Bucket GetBucketRef(int index)
	{
		return ref Unsafe.Add(ref Unsafe.AsRef<Bucket>(_buckets), index);
	}

	private static int NextPowerOfTwo(int value)
	{
		value--;
		value |= value >> 1;
		value |= value >> 2;
		value |= value >> 4;
		value |= value >> 8;
		value |= value >> 16;
		return value + 1;
	}

	[StructLayout(LayoutKind.Sequential)]
	internal struct Bucket
	{
		public ulong Key;
		public Entry* Entries;
		public int Count;
		public int Capacity;
	}

	[StructLayout(LayoutKind.Sequential)]
	public struct Entry
	{
		public int Rid;
		public int TxcIdx;
	}

	public readonly ref struct BucketView
	{
		private readonly Bucket* _bucket;

		internal BucketView(Bucket* bucket)
		{
			_bucket = bucket;
		}

		public ulong Key => _bucket->Key;

		public ReadOnlySpan<Entry> Entries => _bucket->Entries == null
			? ReadOnlySpan<Entry>.Empty
			: new ReadOnlySpan<Entry>(_bucket->Entries, _bucket->Count);
	}

	public ref struct BucketEnumerator
	{
		private readonly Bucket* _buckets;
		private readonly int _capacity;
		private int _index;

		internal BucketEnumerator(Bucket* buckets, int capacity)
		{
			_buckets = buckets;
			_capacity = capacity;
			_index = -1;
		}

		public BucketView Current
		{
			get
			{
				Bucket* bucket = _buckets + _index;
				return new BucketView(bucket);
			}
		}

		public bool MoveNext()
		{
			while (++_index < _capacity)
			{
				Bucket* bucket = _buckets + _index;
				if (bucket->Entries != null && bucket->Count > 0)
				{
					return true;
				}
			}

			return false;
		}
	}
}



