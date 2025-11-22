using OnixRuntime.Api.Maths;
using System.Runtime.CompilerServices;

namespace BoundingBoxes {
    internal static unsafe class Parser {
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static void ParseInto(ReadOnlySpan<byte> data, List<CachedRenderBox> smallDest, List<BoundingBox> largeDest) {
            if (data.Length < 4) return;

            fixed (byte* pinnedData = data) {
                byte* ptr = pinnedData;
                byte* end = pinnedData + data.Length;

                int version = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;
                if (ptr > end) return;

                int structCount = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;
                if (ptr > end) return;

                for (int i = 0; i < structCount; i++) {
                    if (ptr + 6 > end) return;
                    ptr += 4;
                    ushort strLen = Unsafe.ReadUnaligned<ushort>(ptr); ptr += 2;
                    ptr += strLen;
                }
                if (ptr > end) return;

                int aabbCount = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;
                if (ptr > end) return;
                if (aabbCount > 50) return;

                BoundingBox* tempAABBs = stackalloc BoundingBox[aabbCount];
                uint* seenIds = stackalloc uint[aabbCount];
                int aabbActualCount = 0;

                for (int i = 0; i < aabbCount; i++) {
                    if (ptr + 28 > end) return;
                    uint id = Unsafe.ReadUnaligned<uint>(ptr); ptr += 4;

                    bool exists = false;
                    for (int k = 0; k < aabbActualCount; k++) {
                        if (seenIds[k] == id) { exists = true; break; }
                    }

                    if (exists) {
                        ptr += 24;
                        continue;
                    }
                    seenIds[aabbActualCount] = id;

                    int xmin = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;
                    int ymin = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;
                    int zmin = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;
                    int xmax = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;
                    int ymax = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;
                    int zmax = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;

                    tempAABBs[aabbActualCount++] = new BoundingBox(
                        new Vec3(xmin, ymin, zmin),
                        new Vec3(xmax, ymax, zmax)
                    );
                }

                int dynCount = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;
                if (ptr > end) return;
                int dynSkip = dynCount * 8 + dynCount * 4;
                ptr += dynSkip;
                if (ptr > end) return;

                int statCount = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;
                if (ptr > end) return;

                uint* statSeen = stackalloc uint[statCount > 50 ? 50 : statCount];
                int statSeenCount = 0;
                int currentAabbIndex = 0;

                for (int i = 0; i < statCount; i++) {
                    if (ptr + 16 > end) return;
                    uint id = Unsafe.ReadUnaligned<uint>(ptr); ptr += 4;
                    ptr += 4;
                    ptr += 4;
                    int full = Unsafe.ReadUnaligned<int>(ptr); ptr += 4;

                    bool exists = false;
                    for (int k = 0; k < statSeenCount; k++) {
                        if (statSeen[k] == id) { exists = true; break; }
                    }
                    if (exists) continue;
                    if (statSeenCount < 50) statSeen[statSeenCount++] = id;

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