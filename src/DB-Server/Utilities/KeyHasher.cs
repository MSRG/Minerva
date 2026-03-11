using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Minerva.DB_Server;

public static class KeyHasher
{
	// AES-based hash keys (random constants for good diffusion)
	private static readonly Vector128<byte> AesKey1 = Vector128.Create(
		0x2d, 0x35, 0x8d, 0xef, 0x41, 0x94, 0xba, 0x02,
		0x6e, 0x71, 0xc3, 0x9f, 0x12, 0x5a, 0x07, 0xfb);
	private static readonly Vector128<byte> AesKey2 = Vector128.Create(
		0x9e, 0x37, 0x79, 0xb9, 0x7f, 0x4a, 0x7c, 0x15,
		0x6d, 0x58, 0x14, 0x6f, 0x07, 0x0b, 0x5e, 0x8a);

	// Pre-computed keyspace seeds (128-bit vectors with keyspace in lower bits)
	public static readonly Vector128<byte> WarehouseSeed = CreateSeed(KeySpace.Warehouse);
	public static readonly Vector128<byte> DistrictSeed = CreateSeed(KeySpace.District);
	public static readonly Vector128<byte> CustomerSeed = CreateSeed(KeySpace.Customer);
	public static readonly Vector128<byte> ItemSeed = CreateSeed(KeySpace.Item);
	public static readonly Vector128<byte> StockSeed = CreateSeed(KeySpace.Stock);
	public static readonly Vector128<byte> HistorySeed = CreateSeed(KeySpace.History);
	public static readonly Vector128<byte> NewOrderSeed = CreateSeed(KeySpace.NewOrder);
	public static readonly Vector128<byte> OrderSeed = CreateSeed(KeySpace.Order);
	public static readonly Vector128<byte> OrderLineSeed = CreateSeed(KeySpace.OrderLine);
	public static readonly Vector128<byte> YcsbSeed = CreateSeed(KeySpace.Ycsb);

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static Vector128<byte> CreateSeed(KeySpace space)
	{
		// Create a seed with keyspace identifier mixed into random bits
		return Vector128.Create((ulong)space, 0x517cc1b727220a95UL).AsByte();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static ulong AesHashCore(Vector128<byte> state)
	{
		// Two rounds of AES for thorough mixing
		state = Aes.Encrypt(state, AesKey1);
		state = Aes.Encrypt(state, AesKey2);
		// Extract lower 64 bits as final hash
		return state.AsUInt64().GetElement(0);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static ulong AesHashCoreFallback(ulong v0, ulong v1)
	{
		// Fallback using multiplication-based mixing when AES-NI unavailable
		const ulong k0 = 0x9e3779b97f4a7c15UL;
		const ulong k1 = 0xc6a4a7935bd1e995UL;
		ulong h = v0 ^ (v1 * k0);
		h ^= h >> 33;
		h *= k1;
		h ^= h >> 29;
		h *= k0;
		h ^= h >> 32;
		return h;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ulong SingleKey(Vector128<byte> seed, long k1)
	{
		if (Aes.IsSupported)
		{
			var data = Vector128.Create((ulong)k1, 0UL).AsByte();
			var state = Sse2.Xor(seed, data);
			return AesHashCore(state);
		}
		return AesHashCoreFallback(seed.AsUInt64().GetElement(0) ^ (ulong)k1, seed.AsUInt64().GetElement(1));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ulong TwoKeys(Vector128<byte> seed, long k1, long k2)
	{
		if (Aes.IsSupported)
		{
			var data = Vector128.Create((ulong)k1, (ulong)k2).AsByte();
			var state = Sse2.Xor(seed, data);
			return AesHashCore(state);
		}
		return AesHashCoreFallback(seed.AsUInt64().GetElement(0) ^ (ulong)k1, seed.AsUInt64().GetElement(1) ^ (ulong)k2);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ulong ThreeKeys(Vector128<byte> seed, long k1, long k2, long k3)
	{
		if (Aes.IsSupported)
		{
			var data1 = Vector128.Create((ulong)k1, (ulong)k2).AsByte();
			var state = Sse2.Xor(seed, data1);
			state = Aes.Encrypt(state, AesKey1);
			var data2 = Vector128.Create((ulong)k3, 0UL).AsByte();
			state = Sse2.Xor(state, data2);
			return AesHashCore(state);
		}
		ulong h = AesHashCoreFallback(seed.AsUInt64().GetElement(0) ^ (ulong)k1, seed.AsUInt64().GetElement(1) ^ (ulong)k2);
		return AesHashCoreFallback(h, (ulong)k3);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ulong FourKeys(Vector128<byte> seed, long k1, long k2, long k3, long k4)
	{
		if (Aes.IsSupported)
		{
			var data1 = Vector128.Create((ulong)k1, (ulong)k2).AsByte();
			var state = Sse2.Xor(seed, data1);
			state = Aes.Encrypt(state, AesKey1);
			var data2 = Vector128.Create((ulong)k3, (ulong)k4).AsByte();
			state = Sse2.Xor(state, data2);
			return AesHashCore(state);
		}
		ulong h = AesHashCoreFallback(seed.AsUInt64().GetElement(0) ^ (ulong)k1, seed.AsUInt64().GetElement(1) ^ (ulong)k2);
		return AesHashCoreFallback(h ^ (ulong)k3, (ulong)k4);
	}

	public static ulong Ycsb(int shard, string key)
	{
		if (Aes.IsSupported)
		{
			var state = Sse2.Xor(YcsbSeed, Vector128.Create((ulong)(uint)shard, 0UL).AsByte());

			if (!string.IsNullOrEmpty(key))
			{
				int i = 0;
				// Process 8 chars (128 bits) at a time
				for (; i + 8 <= key.Length; i += 8)
				{
					var chars = Vector128.Create(
						(ulong)key[i] | ((ulong)key[i + 1] << 16) | ((ulong)key[i + 2] << 32) | ((ulong)key[i + 3] << 48),
						(ulong)key[i + 4] | ((ulong)key[i + 5] << 16) | ((ulong)key[i + 6] << 32) | ((ulong)key[i + 7] << 48)
					).AsByte();
					state = Aes.Encrypt(state, chars);
				}
				// Handle remaining chars
				ulong lo = 0, hi = 0;
				int shift = 0;
				for (; i < key.Length && shift < 64; i++, shift += 16)
				{
					lo |= (ulong)key[i] << shift;
				}
				shift = 0;
				for (; i < key.Length && shift < 64; i++, shift += 16)
				{
					hi |= (ulong)key[i] << shift;
				}
				if (lo != 0 || hi != 0)
				{
					state = Aes.Encrypt(state, Vector128.Create(lo, hi).AsByte());
				}
			}
			return AesHashCore(state);
		}
		// Fallback
		ulong hash = (ulong)(uint)shard;
		if (!string.IsNullOrEmpty(key))
		{
			foreach (char c in key)
			{
				hash = AesHashCoreFallback(hash, c);
			}
		}
		return hash;
	}

	public static ulong Warehouse(long warehouseId) => SingleKey(WarehouseSeed, warehouseId);
	public static ulong District(long dwid, long did) => TwoKeys(DistrictSeed, dwid, did);
	public static ulong Customer(long cwid, long cdid, long cid) => ThreeKeys(CustomerSeed, cwid, cdid, cid);
	public static ulong Item(long itemId) => SingleKey(ItemSeed, itemId);
	public static ulong Stock(long swid, long siid) => TwoKeys(StockSeed, swid, siid);
	public static ulong History(long hcid, long hdate) => TwoKeys(HistorySeed, hcid, hdate);
	public static ulong NewOrder(long nowid, long nodid, long nooid) => ThreeKeys(NewOrderSeed, nowid, nodid, nooid);
	public static ulong Order(long owid, long odid, long oid) => ThreeKeys(OrderSeed, owid, odid, oid);
	public static ulong OrderLine(long olwId, long oldId, long oloId, long olNumber) => FourKeys(OrderLineSeed, olwId, oldId, oloId, olNumber);

	private enum KeySpace : ulong
	{
		Ycsb = 1,
		Warehouse = 2,
		District = 3,
		Customer = 4,
		Item = 5,
		Stock = 6,
		History = 7,
		NewOrder = 8,
		Order = 9,
		OrderLine = 10,
	}
}

/// <summary>
/// AES-NI accelerated key hasher that can hash multiple keys in parallel.
/// Uses two independent AES pipelines to process 2 keys at once (each AES operates on 128-bit = 2 ulongs).
/// Falls back to scalar AES hashing when batch size is small.
/// </summary>
public static class SimdKeyHasher
{
	// AES round keys for batched hashing
	private static readonly Vector128<byte> AesKey1 = Vector128.Create(
		0x2d, 0x35, 0x8d, 0xef, 0x41, 0x94, 0xba, 0x02,
		0x6e, 0x71, 0xc3, 0x9f, 0x12, 0x5a, 0x07, 0xfb);
	private static readonly Vector128<byte> AesKey2 = Vector128.Create(
		0x9e, 0x37, 0x79, 0xb9, 0x7f, 0x4a, 0x7c, 0x15,
		0x6d, 0x58, 0x14, 0x6f, 0x07, 0x0b, 0x5e, 0x8a);

	public const int SimdWidth = 2; // AES processes 2 keys at once (128-bit block = 2x64-bit)

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static Vector128<byte> AesRound2(Vector128<byte> state)
	{
		state = Aes.Encrypt(state, AesKey1);
		return Aes.Encrypt(state, AesKey2);
	}

	/// <summary>
	/// Hash single-component keys (like Warehouse, Item) using AES-NI.
	/// Processes 2 keys at a time using a single 128-bit AES block.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Hash1ComponentBatch(ReadOnlySpan<long> keys, Span<ulong> hashes, Vector128<byte> seed)
	{
		if (!Aes.IsSupported || keys.Length < SimdWidth)
		{
			Hash1ComponentScalar(keys, hashes, seed);
			return;
		}

		int i = 0;
		// Process 2 keys at a time
		for (; i + SimdWidth <= keys.Length; i += SimdWidth)
		{
			// Pack two keys into one 128-bit block, XOR with seed, then AES hash
			var data = Vector128.Create((ulong)keys[i], (ulong)keys[i + 1]).AsByte();
			var state = Sse2.Xor(seed, data);
			var result = AesRound2(state).AsUInt64();
			hashes[i] = result.GetElement(0);
			hashes[i + 1] = result.GetElement(1);
		}

		// Handle remainder
		for (; i < keys.Length; i++)
		{
			hashes[i] = KeyHasher.SingleKey(seed, keys[i]);
		}
	}

	/// <summary>
	/// Hash two-component keys (like District, Stock, History) using AES-NI.
	/// Each key pair fits perfectly in one 128-bit AES block.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Hash2ComponentBatch(ReadOnlySpan<(long, long)> keys, Span<ulong> hashes, Vector128<byte> seed)
	{
		if (!Aes.IsSupported)
		{
			Hash2ComponentScalar(keys, hashes, seed);
			return;
		}

		// Each 2-component key fills a full 128-bit block perfectly
		for (int i = 0; i < keys.Length; i++)
		{
			var data = Vector128.Create((ulong)keys[i].Item1, (ulong)keys[i].Item2).AsByte();
			var state = Sse2.Xor(seed, data);
			hashes[i] = AesRound2(state).AsUInt64().GetElement(0);
		}
	}

	/// <summary>
	/// Hash three-component keys (like Customer, NewOrder, Order) using AES-NI.
	/// Uses two AES rounds: first mixes k1,k2, second adds k3.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Hash3ComponentBatch(ReadOnlySpan<(long, long, long)> keys, Span<ulong> hashes, Vector128<byte> seed)
	{
		if (!Aes.IsSupported)
		{
			Hash3ComponentScalar(keys, hashes, seed);
			return;
		}

		for (int i = 0; i < keys.Length; i++)
		{
			var data1 = Vector128.Create((ulong)keys[i].Item1, (ulong)keys[i].Item2).AsByte();
			var state = Sse2.Xor(seed, data1);
			state = Aes.Encrypt(state, AesKey1);
			var data2 = Vector128.Create((ulong)keys[i].Item3, 0UL).AsByte();
			state = Sse2.Xor(state, data2);
			hashes[i] = AesRound2(state).AsUInt64().GetElement(0);
		}
	}

	/// <summary>
	/// Hash four-component keys (like OrderLine) using AES-NI.
	/// Uses two AES rounds: first mixes k1,k2, second mixes k3,k4.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Hash4ComponentBatch(ReadOnlySpan<(long, long, long, long)> keys, Span<ulong> hashes, Vector128<byte> seed)
	{
		if (!Aes.IsSupported)
		{
			Hash4ComponentScalar(keys, hashes, seed);
			return;
		}

		for (int i = 0; i < keys.Length; i++)
		{
			var data1 = Vector128.Create((ulong)keys[i].Item1, (ulong)keys[i].Item2).AsByte();
			var state = Sse2.Xor(seed, data1);
			state = Aes.Encrypt(state, AesKey1);
			var data2 = Vector128.Create((ulong)keys[i].Item3, (ulong)keys[i].Item4).AsByte();
			state = Sse2.Xor(state, data2);
			hashes[i] = AesRound2(state).AsUInt64().GetElement(0);
		}
	}

	// Scalar fallbacks using KeyHasher's AES-based methods
	private static void Hash1ComponentScalar(ReadOnlySpan<long> keys, Span<ulong> hashes, Vector128<byte> seed)
	{
		for (int i = 0; i < keys.Length; i++)
		{
			hashes[i] = KeyHasher.SingleKey(seed, keys[i]);
		}
	}

	private static void Hash2ComponentScalar(ReadOnlySpan<(long, long)> keys, Span<ulong> hashes, Vector128<byte> seed)
	{
		for (int i = 0; i < keys.Length; i++)
		{
			hashes[i] = KeyHasher.TwoKeys(seed, keys[i].Item1, keys[i].Item2);
		}
	}

	private static void Hash3ComponentScalar(ReadOnlySpan<(long, long, long)> keys, Span<ulong> hashes, Vector128<byte> seed)
	{
		for (int i = 0; i < keys.Length; i++)
		{
			hashes[i] = KeyHasher.ThreeKeys(seed, keys[i].Item1, keys[i].Item2, keys[i].Item3);
		}
	}

	private static void Hash4ComponentScalar(ReadOnlySpan<(long, long, long, long)> keys, Span<ulong> hashes, Vector128<byte> seed)
	{
		for (int i = 0; i < keys.Length; i++)
		{
			hashes[i] = KeyHasher.FourKeys(seed, keys[i].Item1, keys[i].Item2, keys[i].Item3, keys[i].Item4);
		}
	}
}