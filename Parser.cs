using OnixRuntime.Api.Maths;
using System.Runtime.CompilerServices;

namespace BoundingBoxes {
    internal static unsafe class Parser {
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static void ParseAABBVolumes(ReadOnlySpan<byte> data, List<CachedRenderBox> smallDest, List<BoundingBox> largeDest) {
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
                if (statCount > 50) return; 
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
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static void ParseVillageInfo(ReadOnlySpan<byte> data, List<BoundingBox> dest) {
            if (data.Length < 8) return;

            fixed (byte* pinned = data) {
                byte* ptr = pinned;
                byte* end = pinned + data.Length;
                if (*ptr != 0x0A) return;
                ptr++;
                ptr += 2;

                bool gotX0 = false, gotY0 = false, gotZ0 = false;
                bool gotX1 = false, gotY1 = false, gotZ1 = false;

                int X0 = 0, Y0 = 0, Z0 = 0;
                int X1 = 0, Y1 = 0, Z1 = 0;

                while (ptr < end) {
                    byte tag = *ptr++;
                    if (tag == 0) break;
                    if (ptr + 2 > end) return;
                    ushort nameLen = Unsafe.ReadUnaligned<ushort>(ptr);
                    ptr += 2;
                    if (ptr + nameLen > end) return;
                    byte* namePtr = ptr;
                    ptr += nameLen;
                    switch (nameLen) {
                        case 2: {
                                byte a = namePtr[0];
                                byte b = namePtr[1];
                                if (tag != 0x03) {
                                    if (ptr + 4 > end) return;
                                    ptr += 4;
                                    continue;
                                }

                                int val = Unsafe.ReadUnaligned<int>(ptr);
                                ptr += 4;

                                if (a == 'X' && b == '0') { X0 = val; gotX0 = true; } else if (a == 'X' && b == '1') { X1 = val; gotX1 = true; } else if (a == 'Y' && b == '0') { Y0 = val; gotY0 = true; } else if (a == 'Y' && b == '1') { Y1 = val; gotY1 = true; } else if (a == 'Z' && b == '0') { Z0 = val; gotZ0 = true; } else if (a == 'Z' && b == '1') { Z1 = val; gotZ1 = true; }
                                break;
                            }

                        default:
                            switch (tag) {
                                case 1: ptr += 1; break;
                                case 2: ptr += 2; break;
                                case 3: ptr += 4; break;
                                case 4: ptr += 8; break;
                                case 5: ptr += 4; break;
                                case 6: ptr += 8; break; 
                                case 7: {
                                        if (ptr + 4 > end) return;
                                        int len = Unsafe.ReadUnaligned<int>(ptr);
                                        ptr += 4 + len;
                                        break;
                                    }
                                case 8: { 
                                        if (ptr + 2 > end) return;
                                        ushort sl = Unsafe.ReadUnaligned<ushort>(ptr);
                                        ptr += 2 + sl;
                                        break;
                                    }
                                case 9: { 
                                        if (ptr + 5 > end) return;
                                        byte inner = *ptr++;
                                        int count = Unsafe.ReadUnaligned<int>(ptr);
                                        ptr += 4;
                                        return;
                                    }
                                case 10: {
                                        return;
                                    }
                                default:
                                    return;
                            }
                            break;
                    }

                    if (gotX0 && gotY0 && gotZ0 &&
                        gotX1 && gotY1 && gotZ1) {
                        dest.Add(new BoundingBox(
                            new Vec3(X0, Y0, Z0),
                            new Vec3(X1, Y1, Z1)
                        ));
                        return;
                    }
                }
            }
        }


    }
}