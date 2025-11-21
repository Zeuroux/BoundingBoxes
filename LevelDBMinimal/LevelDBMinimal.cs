using OnixRuntime.Plugin;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

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
            if (handle == IntPtr.Zero) {
                int err = Marshal.GetLastWin32Error();
                throw new Exception($"LoadLibrary failed with error {err}");
            }

            FreeLibrary(handle);
            FreeLibrary(handle);
        }
        #region PInvoke - Main DB

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
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool GetValue(
            IntPtr db,
            byte* key,
            UIntPtr keyLen,
            out byte* outVal,
            out UIntPtr outLen);

        #endregion

        #region PInvoke - Log Session

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static partial IntPtr OpenLogSession(byte* dbPath);

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        private static partial void CloseLogSession(IntPtr session);

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool GetValueFromSession(
            IntPtr session,
            byte* key,
            UIntPtr keyLen,
            out byte* outVal,
            out UIntPtr outLen);

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        [return: MarshalAs(UnmanagedType.I1)]
        private static partial bool UpdateLogSession(IntPtr session, byte* logDir);

        #endregion

        #region Utilities

        [LibraryImport(Dll)]
        [UnmanagedCallConv(CallConvs = new Type[] { typeof(CallConvCdecl) })]
        internal static partial void FreeBuffer(byte* buffer);

        #endregion

        #region Main DB Wrapper

        public IntPtr NativeHandle => _nativeDb;

        public LevelDBMinimal(string path) {
            byte[] pathBytes = Encoding.UTF8.GetBytes(path + "\0");
            fixed (byte* p = pathBytes) {
                _nativeDb = OpenDB(p);
            }
        }

        public void Dispose() {
            if (_nativeDb != IntPtr.Zero) {
                CloseDB(_nativeDb);
                _nativeDb = IntPtr.Zero;
            }
            GC.SuppressFinalize(this);
        }

        public bool Update(string path) {
            byte[] pathBytes = Encoding.UTF8.GetBytes(path + "\0");
            fixed (byte* p = pathBytes) {
                return UpdateDB(_nativeDb, p);
            }
        }
        #endregion

        #region Log Session Wrapper

        public class LogSession : IDisposable {
            private IntPtr _sessionPtr;
            public IntPtr NativeHandle => _sessionPtr;

            public LogSession(string dbPath) {
                byte[] pathBytes = Encoding.UTF8.GetBytes(dbPath + "\0");
                fixed (byte* p = pathBytes) {
                    _sessionPtr = OpenLogSession(p);
                }
            }

            public bool Update(string logDir) {
                if (_sessionPtr == IntPtr.Zero) return false;
                byte[] pathBytes = Encoding.UTF8.GetBytes(logDir + "\0");
                fixed (byte* p = pathBytes) {
                    return UpdateLogSession(_sessionPtr, p);
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
        #endregion
    }
}