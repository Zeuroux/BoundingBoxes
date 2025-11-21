using OnixRuntime.Api.Maths;
using System.Runtime.CompilerServices;

namespace BoundingBoxes {
    internal static unsafe class Parser {

        public static void ParseInto(ReadOnlySpan<byte> data, List<CachedRenderBox> smallDest, List<BoundingBox> largeDest) {
            if (data.Length < 4) return;

            fixed (byte* pinnedData = data) {
                byte* ptr = pinnedData;
                byte* end = pinnedData + data.Length;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static bool TryReadI32(ref byte* p, byte* e, out int value) {
                    if (p + 4 > e) { value = default; return false; }
                    value = Unsafe.ReadUnaligned<int>(p);
                    p += 4;
                    return true;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static bool TryReadU32(ref byte* p, byte* e, out uint value) {
                    if (p + 4 > e) { value = default; return false; }
                    value = Unsafe.ReadUnaligned<uint>(p);
                    p += 4;
                    return true;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                static bool TryReadU16(ref byte* p, byte* e, out ushort value) {
                    if (p + 2 > e) { value = default; return false; }
                    value = Unsafe.ReadUnaligned<ushort>(p);
                    p += 2;
                    return true;
                }

                if (!TryReadI32(ref ptr, end, out int version)) return;

                if (!TryReadI32(ref ptr, end, out int structCount)) return;
                for (int i = 0; i < structCount; i++) {
                    if (!TryReadU32(ref ptr, end, out _)) return;
                    if (!TryReadU16(ref ptr, end, out ushort strLen)) return;
                    ptr += strLen;
                    if (ptr > end) return;
                }

                if (!TryReadI32(ref ptr, end, out int aabbCount)) return;
                if (aabbCount > 50) {
                    Console.WriteLine(aabbCount);
                    return;
                }
                var tempAABBs = new BoundingBox[aabbCount];
                int aabbActualCount = 0;
                var aabbSeen = new HashSet<uint>(aabbCount > 0 ? aabbCount : 0);

                for (int i = 0; i < aabbCount; i++) {
                    if (!TryReadU32(ref ptr, end, out uint id)) return;

                    if (!aabbSeen.Add(id)) {
                        ptr += 24;
                        if (ptr > end) return;
                        continue;
                    }

                    if (!TryReadI32(ref ptr, end, out int xmin)) return;
                    if (!TryReadI32(ref ptr, end, out int ymin)) return;
                    if (!TryReadI32(ref ptr, end, out int zmin)) return;
                    if (!TryReadI32(ref ptr, end, out int xmax)) return;
                    if (!TryReadI32(ref ptr, end, out int ymax)) return;
                    if (!TryReadI32(ref ptr, end, out int zmax)) return;

                    tempAABBs[aabbActualCount++] = new BoundingBox(
                        new Vec3(xmin, ymin, zmin),
                        new Vec3(xmax, ymax, zmax)
                    );
                }

                if (!TryReadI32(ref ptr, end, out int dynCount)) return;
                for (int i = 0; i < dynCount; i++) {
                    if (!TryReadU32(ref ptr, end, out _)) return;
                    ptr += 8;
                    if (ptr > end) return;
                }

                if (!TryReadI32(ref ptr, end, out int statCount)) return;
                var statSeen = new HashSet<uint>(statCount > 0 ? statCount : 0);

                int currentAabbIndex = 0;
                for (int i = 0; i < statCount; i++) {
                    if (!TryReadU32(ref ptr, end, out uint id)) return;
                    if (!TryReadU32(ref ptr, end, out _)) return;
                    if (!TryReadI32(ref ptr, end, out _)) return;
                    if (!TryReadI32(ref ptr, end, out int full)) return;

                    if (!statSeen.Add(id)) continue;

                    if (currentAabbIndex < aabbActualCount) {
                        var box = tempAABBs[currentAabbIndex];
                        if (full != 0) {
                            largeDest.Add(box);
                        } else {
                            smallDest.Add(new CachedRenderBox(box));
                        }
                        currentAabbIndex++;
                    }
                }

                while (currentAabbIndex < aabbActualCount) {
                    smallDest.Add(new CachedRenderBox(tempAABBs[currentAabbIndex]));
                    currentAabbIndex++;
                }
            }
        }
    }
}
