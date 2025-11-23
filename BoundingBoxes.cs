using OnixRuntime.Api;
using OnixRuntime.Api.Entities;
using OnixRuntime.Api.Maths;
using OnixRuntime.Api.Rendering;
using OnixRuntime.Plugin;
using System.Runtime.CompilerServices;
using System.Text;

namespace BoundingBoxes {
    internal readonly struct CachedRenderBox {
        public readonly BoundingBox MainBox;
        public readonly BoundingBox CenterBox;
        public readonly Vec3 LineStart;
        public readonly Vec3 LineEnd;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CachedRenderBox(BoundingBox box) {
            MainBox = new BoundingBox(box.Minimum, box.Maximum + 1);
            var size = box.Maximum - box.Minimum;
            var center = box.Minimum + size / 2;
            var renderPos = center;
            if (size.X % 2 != 0) renderPos.X += 0.5f;
            if (size.Z % 2 != 0) renderPos.Z += 0.5f;
            renderPos.Y = box.Minimum.Y;
            CenterBox = new BoundingBox(renderPos, new Vec3(renderPos.X + 1, box.Maximum.Y + 1, renderPos.Z + 1));
            LineStart = CenterBox.Minimum;
            LineEnd = new Vec3(renderPos.X, box.Maximum.Y, renderPos.Z);
        }
    }

    public unsafe class BoundingBoxes : OnixPluginBase {
        public static BoundingBoxes Instance { get; private set; } = null!;
        public static BoundingBoxesConfig Config { get; private set; } = null!;

        private LevelDBMinimal? db;
        private LevelDBMinimal.LogSession? dbls;

        private volatile CachedRenderBox[] renderCache = [];
        private volatile BoundingBox[] largeBoxes = [];

        private Thread? workerThread;
        private readonly ManualResetEventSlim updateSignal = new(false);
        private CancellationTokenSource? cts;
        private volatile bool needsUpdate;

        private struct PlayerChunkInfo {
            public int X, Z, Dimension;
        }

        private PlayerChunkInfo lastPlayerChunk;
        private const string srcPath = @"C:\Users\Zeyro\AppData\Roaming\Minecraft Bedrock\Users\14394695988390012034\games\com.mojang\minecraftWorlds\rOawr6mqbUc=\db";

        public BoundingBoxes(OnixPluginInitInfo initInfo) : base(initInfo) {
            Instance = this;
            base.DisablingShouldUnloadPlugin = false;
        }
        protected override void OnLoaded() {
            Config = new BoundingBoxesConfig(PluginDisplayModule, true);
            Onix.Events.Common.WorldRender += OnWorldRender;
            Onix.Events.Common.Tick += OnTick;
            try {    
                db = new LevelDBMinimal(srcPath);
                dbls = new LevelDBMinimal.LogSession(srcPath);
            } catch { }
        }

        protected override void OnEnabled() {
            if (db is null || dbls is null) return;
            if (cts != null) OnDisabled();
            cts = new();
            workerThread = new Thread(() => BackgroundWorker(cts.Token)) {
                IsBackground = true,
                Priority = ThreadPriority.Highest 
            };
            workerThread.Start();
        }

        protected override void OnDisabled() {
            if (cts != null) {
                cts.Cancel();
                updateSignal.Set();
                workerThread?.Join(500);
                cts.Dispose();
                cts = null;
                workerThread = null;
            }
        }
        protected override void OnUnloaded() {
            OnDisabled();
            db?.Dispose();
            dbls?.Dispose();
            updateSignal.Dispose();
            LevelDBMinimal.Unload();
            Onix.Events.Common.WorldRender -= OnWorldRender;
            Onix.Events.Common.Tick -= OnTick;
        }
        int counter = 0;
        private void OnTick() {
            counter++;
            if (counter % 10 != 0) return;
            if (Onix.LocalPlayer is not LocalPlayer lp) return;
            int cx = lp.ChunkPosition.X;
            int cz = lp.ChunkPosition.Y;
            int dim = (int)lp.Dimension.Id;
            
            lastPlayerChunk = new PlayerChunkInfo { X = cx, Z = cz, Dimension = dim };
            needsUpdate = true;
            updateSignal.Set();
        }

        private void BackgroundWorker(CancellationToken token) {
            var tempBoxes = new List<CachedRenderBox>(256);
            var tempLargeBoxes = new List<BoundingBox>(64);
            const int radius = 7;
            const int diameter = (radius * 2) + 1;
            const int area = diameter * diameter;
            
            const int maxKeySize = 13; 
            byte[] keysBuffer = new byte[area * maxKeySize];
            int[] keyOffsets = new int[area];
            int[] keyLengths = new int[area];
            
            int[] outOffsets = new int[area];
            int[] outLengths = new int[area];
            byte[] outFound = new byte[area];

            while (!token.IsCancellationRequested) {
                try {
                    updateSignal.Wait(token);
                    updateSignal.Reset();
                    if (!needsUpdate) continue;
                    needsUpdate = false;

                    if (db is null || dbls is null) continue;
                    db.Update(srcPath);
                    dbls.Update(srcPath);

                    tempBoxes.Clear();
                    tempLargeBoxes.Clear();

                    var chunkInfo = lastPlayerChunk;
                    int keyCount = 0;
                    int currentKeyOffset = 0;

                    fixed (byte* pBase = keysBuffer) {
                        for (int dx = -radius; dx <= radius; dx++) {
                            for (int dz = -radius; dz <= radius; dz++) {
                                int x = chunkInfo.X + dx;
                                int z = chunkInfo.Z + dz;
                                int dim = chunkInfo.Dimension;
                                
                                int kLen = (dim != 0) ? 13 : 9;
                                byte* ptr = pBase + currentKeyOffset;
                                
                                Unsafe.WriteUnaligned(ptr, x);
                                Unsafe.WriteUnaligned(ptr + 4, z);
                                int offset = 8;
                                if (dim != 0) {
                                    Unsafe.WriteUnaligned(ptr + offset, dim);
                                    offset += 4;
                                }
                                *(ptr + offset) = 0x77;
                                
                                keyOffsets[keyCount] = currentKeyOffset;
                                keyLengths[keyCount] = kLen;
                                currentKeyOffset += kLen;
                                keyCount++;
                            }
                        }
                    }

                    if (token.IsCancellationRequested) break;

                    static void HandleBatch(nint ptr, int[] oOff, int[] oLen, byte[] found, int cnt, List<CachedRenderBox> boxes, List<BoundingBox> largeBoxes) {
                        for (int i = 0; i < cnt; i++) {
                            if (found[i] != 0 && oLen[i] > 0) {
                                Parser.ParseAABBVolumes(new ReadOnlySpan<byte>((byte*)ptr + oOff[i], oLen[i]), boxes, largeBoxes);
                            }
                        }
                    }

                    db.BatchGetRaw(keysBuffer, keyOffsets, keyLengths, keyCount, outOffsets, outLengths, outFound,
                        (ptr, oOff, oLen, found, cnt) => HandleBatch(ptr, oOff, oLen, found, cnt, tempBoxes, tempLargeBoxes));

                    bool hasMissing = false;
                    for (int i = 0; i < keyCount; i++) if (outFound[i] == 0) { hasMissing = true; break; }

                    if (hasMissing) {
                        dbls.BatchGetRaw(keysBuffer, keyOffsets, keyLengths, keyCount, outOffsets, outLengths, outFound,
                            (ptr, oOff, oLen, found, cnt) => HandleBatch(ptr, oOff, oLen, found, cnt, tempBoxes, tempLargeBoxes));
                    }
                    db.Iterate("VILLAGE", "INFO", (keySpan, valSpan) => {
                        Parser.ParseVillageInfo(valSpan, tempLargeBoxes);
                    });
                    renderCache = [.. tempBoxes];
                    largeBoxes = [.. tempLargeBoxes];
                } catch (OperationCanceledException) {
                    break;
                } catch { }
            }
        }


        private void OnWorldRender(RendererWorld gfx, float delta) {
            var currentCache = renderCache;
            var currentLarge = largeBoxes;
            int cCount = currentCache.Length;
            int lCount = currentLarge.Length;
            
            for (int i = 0; i < lCount; i++) {
                var box = currentLarge[i];
                gfx.RenderBoundingBoxOutline(new BoundingBox(box.Minimum, box.Maximum + 1), ColorF.Red);
            }
            for (int i = 0; i < cCount; i++) {
                ref var item = ref currentCache[i];
                gfx.RenderBoundingBoxOutline(item.MainBox, ColorF.Aqua);
                gfx.DrawLine(item.LineStart, item.LineEnd, ColorF.Red);
                gfx.RenderBoundingBoxOutline(item.CenterBox, ColorF.White);
            }
        }
    }
}