using OnixRuntime.Api;
using OnixRuntime.Api.Entities;
using OnixRuntime.Api.Maths;
using OnixRuntime.Api.Rendering;
using OnixRuntime.Plugin;
using System.Runtime.CompilerServices;

namespace BoundingBoxes {
    internal readonly struct CachedRenderBox {
        public readonly BoundingBox MainBox;
        public readonly BoundingBox CenterBox;
        public readonly Vec3 LineStart;
        public readonly Vec3 LineEnd;

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
            public readonly bool Equals(PlayerChunkInfo other) => X == other.X && Z == other.Z && Dimension == other.Dimension;
        }

        private PlayerChunkInfo lastPlayerChunk;
        private const string srcPath = @"C:\Users\Zeyro\AppData\Roaming\Minecraft Bedrock\Users\14394695988390012034\games\com.mojang\minecraftWorlds\nYGbgsdjPN4=\db";

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
            } catch (Exception ex) {
                Console.WriteLine($"Failed to init LevelDB: {ex.Message}");
            }
        }

        protected override void OnEnabled() {
            if (db is null || dbls is null) return;
            if (cts != null) OnDisabled();
            cts = new();
            workerThread = new Thread(() => BackgroundWorker(cts.Token)) {
                IsBackground = true,
                Name = "BoundingBoxLoader",
                Priority = ThreadPriority.Normal
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
        private void OnTick() {
            if (Onix.LocalPlayer is not LocalPlayer lp) return;
            var currentChunk = new PlayerChunkInfo {
                X = lp.ChunkPosition.X,
                Z = lp.ChunkPosition.Y,
                Dimension = (int)lp.Dimension.Id
            };
            lastPlayerChunk = currentChunk;
            needsUpdate = true;
            updateSignal.Set();
        }

        private void BackgroundWorker(CancellationToken token) {
            var tempBoxes = new List<CachedRenderBox>(256);
            var tempLargeBoxes = new List<BoundingBox>(64);
            const int radius = 4;

            byte* keyBuf = stackalloc byte[16];

            while (!token.IsCancellationRequested) {
                try {
                    updateSignal.Wait(token);
                    updateSignal.Reset();

                    if (!needsUpdate) continue;
                    needsUpdate = false;

                    if (db is null || dbls is null) continue;

                    IntPtr dbHandle = db.NativeHandle;
                    IntPtr logHandle = dbls.NativeHandle;

                    db.Update(srcPath);
                    dbls.Update(srcPath);

                    tempBoxes.Clear();
                    tempLargeBoxes.Clear();

                    var chunkInfo = lastPlayerChunk;

                    for (int dx = -radius; dx <= radius; dx++) {
                        for (int dz = -radius; dz <= radius; dz++) {
                            if (token.IsCancellationRequested) return;

                            int keyLen = WriteChunkKey(keyBuf, chunkInfo.X + dx, chunkInfo.Z + dz, chunkInfo.Dimension);

                            if (LevelDBMinimal.GetValue(dbHandle, keyBuf, (UIntPtr)keyLen, out byte* valPtr, out nuint valLen)) {
                                var dataSpan = new ReadOnlySpan<byte>(valPtr, (int)valLen);
                                Parser.ParseInto(dataSpan, tempBoxes, tempLargeBoxes);
                                LevelDBMinimal.FreeBuffer(valPtr);
                            } else if (LevelDBMinimal.GetValueFromSession(logHandle, keyBuf, (UIntPtr)keyLen, out valPtr, out valLen)) {
                                var dataSpan = new ReadOnlySpan<byte>(valPtr, (int)valLen);
                                Parser.ParseInto(dataSpan, tempBoxes, tempLargeBoxes);
                                LevelDBMinimal.FreeBuffer(valPtr);
                            }
                        }
                    }

                    renderCache = [.. tempBoxes];
                    largeBoxes = [.. tempLargeBoxes];
                } catch (OperationCanceledException) {
                    break;
                }
            }
        }

        private void OnWorldRender(RendererWorld gfx, float delta) {
            var currentCache = renderCache;
            var currentLarge = largeBoxes;

            foreach (var box in currentLarge) {
                gfx.RenderBoundingBoxOutline(new BoundingBox(box.Minimum, box.Maximum + 1), ColorF.Red);
            }
            foreach (var item in currentCache) {
                gfx.RenderBoundingBoxOutline(item.MainBox, ColorF.Aqua);
                gfx.DrawLine(item.LineStart, item.LineEnd, ColorF.Red);
                gfx.RenderBoundingBoxOutline(item.CenterBox, ColorF.White);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int WriteChunkKey(byte* ptr, int x, int z, int dimension) {
            Unsafe.WriteUnaligned(ptr, x);
            Unsafe.WriteUnaligned(ptr + 4, z);

            int offset = 8;
            if (dimension != 0) {
                Unsafe.WriteUnaligned(ptr + offset, dimension);
                offset += 4;
            }
            *(ptr + offset) = 0x77;
            return offset + 1;
        }
    }
}