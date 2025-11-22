using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Buffers;

namespace BoundingBoxes {
    public unsafe partial class LevelDBMinimal : IDisposable {
        private IntPtr _nativeDb;
        private const string Dll = "LevelDBMinimal/LevelDBMinimal";

        [LibraryImport("kernel32", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
        private static partial IntPtr LoadLibraryW(string lpFileName);

        [LibraryImport("kernel32", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static partial bool FreeLibrary(IntPtr hModule);

        public static void Unload() {
            IntPtr handle = LoadLibraryW("LevelDBMinimal.dll");
            if (handle != IntPtr.Zero) {
                FreeLibrary(handle);
                FreeLibrary(handle);
            }
        }

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static partial IntPtr OpenDB(byte* path);

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        [return: MarshalAs(UnmanagedType.I1)]
        private static partial bool UpdateDB(IntPtr db, byte* path);

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static partial void CloseDB(IntPtr db);

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static partial void BatchGetFlat(
            IntPtr db,
            byte* flatKeys,
            int* keyOffsets,
            int* keyLengths,
            int count,
            out byte* outDataBlock,
            int* outDataOffsets,
            int* outDataLengths,
            byte* outFound
        );

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static partial IntPtr OpenLogSession(byte* dbPath);

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static partial void CloseLogSession(IntPtr session);

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        [return: MarshalAs(UnmanagedType.I1)]
        private static partial bool UpdateLogSession(IntPtr session, byte* logDir);

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static partial void BatchGetSessionFlat(
            IntPtr session,
            byte* flatKeys,
            int* keyOffsets,
            int* keyLengths,
            int count,
            out byte* outDataBlock,
            int* outDataOffsets,
            int* outDataLengths,
            byte* outFound
        );

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static partial void FreeBuffer(byte* buffer);

        public IntPtr NativeHandle => _nativeDb;

        public LevelDBMinimal(string path) {
            var utf8ByteCount = Encoding.UTF8.GetByteCount(path);
            Span<byte> buffer = stackalloc byte[utf8ByteCount + 1];
            Encoding.UTF8.GetBytes(path, buffer);
            buffer[utf8ByteCount] = 0;
            fixed (byte* p = buffer) { _nativeDb = OpenDB(p); }
        }

        public void Dispose() {
            if (_nativeDb != IntPtr.Zero) {
                CloseDB(_nativeDb);
                _nativeDb = IntPtr.Zero;
            }
            GC.SuppressFinalize(this);
        }

        public bool Update(string path) {
            var utf8ByteCount = Encoding.UTF8.GetByteCount(path);
            Span<byte> buffer = stackalloc byte[utf8ByteCount + 1];
            Encoding.UTF8.GetBytes(path, buffer);
            buffer[utf8ByteCount] = 0;
            fixed (byte* p = buffer) { return UpdateDB(_nativeDb, p); }
        }

        public void BatchGetRaw(
            ReadOnlySpan<byte> flatKeys, 
            ReadOnlySpan<int> keyOffsets, 
            ReadOnlySpan<int> keyLengths, 
            int count,
            int[] outOffsets,
            int[] outLengths,
            byte[] outFound,
            Action<IntPtr, int[], int[], byte[], int> resultHandler) {
            
            byte* pResultBlock = null;
            fixed (byte* pFlatKeys = flatKeys)
            fixed (int* pKeyOffsets = keyOffsets)
            fixed (int* pKeyLengths = keyLengths)
            fixed (int* pOutOffsets = outOffsets)
            fixed (int* pOutLengths = outLengths)
            fixed (byte* pOutFound = outFound) {
                BatchGetFlat(_nativeDb, pFlatKeys, pKeyOffsets, pKeyLengths, count, out pResultBlock, pOutOffsets, pOutLengths, pOutFound);
                if (pResultBlock != null) {
                    resultHandler((nint)pResultBlock, outOffsets, outLengths, outFound, count);
                    FreeBuffer(pResultBlock);
                }
            }
        }

        public class LogSession : IDisposable {
            private IntPtr _sessionPtr;
            public IntPtr NativeHandle => _sessionPtr;

            public LogSession(string dbPath) {
                var utf8ByteCount = Encoding.UTF8.GetByteCount(dbPath);
                Span<byte> buffer = stackalloc byte[utf8ByteCount + 1];
                Encoding.UTF8.GetBytes(dbPath, buffer);
                buffer[utf8ByteCount] = 0;
                fixed (byte* p = buffer) { _sessionPtr = OpenLogSession(p); }
            }

            public bool Update(string logDir) {
                if (_sessionPtr == IntPtr.Zero) return false;
                var utf8ByteCount = Encoding.UTF8.GetByteCount(logDir);
                Span<byte> buffer = stackalloc byte[utf8ByteCount + 1];
                Encoding.UTF8.GetBytes(logDir, buffer);
                buffer[utf8ByteCount] = 0;
                fixed (byte* p = buffer) { return UpdateLogSession(_sessionPtr, p); }
            }

            public void BatchGetRaw(
                ReadOnlySpan<byte> flatKeys,
                ReadOnlySpan<int> keyOffsets,
                ReadOnlySpan<int> keyLengths,
                int count,
                int[] outOffsets,
                int[] outLengths,
                byte[] outFound,
                Action<IntPtr, int[], int[], byte[], int> resultHandler) {

                byte* pResultBlock = null;
                fixed (byte* pFlatKeys = flatKeys)
                fixed (int* pKeyOffsets = keyOffsets)
                fixed (int* pKeyLengths = keyLengths)
                fixed (int* pOutOffsets = outOffsets)
                fixed (int* pOutLengths = outLengths)
                fixed (byte* pOutFound = outFound) {
                    BatchGetSessionFlat(_sessionPtr, pFlatKeys, pKeyOffsets, pKeyLengths, count, out pResultBlock, pOutOffsets, pOutLengths, pOutFound);
                    if (pResultBlock != null) {
                        resultHandler((nint)pResultBlock, outOffsets, outLengths, outFound, count);
                        FreeBuffer(pResultBlock);
                    }
                }
            }

            public void Dispose() {
                if (_sessionPtr != IntPtr.Zero) {
                    CloseLogSession(_sessionPtr);
                    _sessionPtr = IntPtr.Zero;
                }
                GC.SuppressFinalize(this);
            }
        }
    }
}