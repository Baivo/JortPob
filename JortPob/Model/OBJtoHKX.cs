using HKLib.hk2018;
using HKLib.Reflection.hk2018;
using HKLib.Serialization.hk2018.Binary;
using HKLib.Serialization.hk2018.Xml;
using JortPob.Common;
using SoulsFormats;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/* Code here is courtesy of Dropoff */
/* Also uses some stuff by Hork & 12th I think */
/* This is a modified version of ER_OBJ2HKX */
namespace JortPob.Model
{
    partial class ModelConverter
    {
        // PERF: cache tool directory path
        private static readonly string s_toolDir = $"{AppDomain.CurrentDomain.BaseDirectory}Resources\\tools\\ER_OBJ2HKX\\";
        private static readonly string s_workRoot = Path.Combine(s_toolDir, "work");

        // PERF: temp dir pool to reduce FS ops
        private static readonly ConcurrentBag<string> s_tempDirPool = new ConcurrentBag<string>();
        private static readonly int s_maxTempDirs = Math.Max(2, Environment.ProcessorCount / 2);

        // PERF: cache Havok registry once per process
        private static readonly Lazy<HavokTypeRegistry> s_registry =
            new Lazy<HavokTypeRegistry>(
                () => HavokTypeRegistry.Load(Path.Combine(s_toolDir, "HavokTypeRegistry20180100.xml")),
                LazyThreadSafetyMode.ExecutionAndPublication);

        // PERF: thread-local serializers for parallel callers
        private static readonly ThreadLocal<HavokBinarySerializer> s_binSerializer =
            new ThreadLocal<HavokBinarySerializer>(() => new HavokBinarySerializer(s_registry.Value), trackAllValues: false);

        private static readonly ThreadLocal<HavokXmlSerializer> s_xmlSerializer =
            new ThreadLocal<HavokXmlSerializer>(() => new HavokXmlSerializer(s_registry.Value), trackAllValues: false);

        // PERF: thread-local deserializer to avoid per-call allocations
        private static readonly ThreadLocal<HKX2.PackFileDeserializer> s_deserializer =
            new ThreadLocal<HKX2.PackFileDeserializer>(() => new HKX2.PackFileDeserializer(), trackAllValues: false);

        static ModelConverter()
        {
            // Initialize temp dir pool
            Directory.CreateDirectory(s_workRoot);
            for (int i = 0; i < s_maxTempDirs; i++)
            {
                string dir = Path.Combine(s_workRoot, $"pool_{i}");
                if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
                Directory.CreateDirectory(dir);
                s_tempDirPool.Add(dir);
            }
        }

        // NEW: batch API with temp dir pooling and async I/O
        public static async Task OBJtoHKXBatch(IEnumerable<(string objPath, string hkxPath)> items, int? maxDegree = null, CancellationToken cancellationToken = default, Action? progressCallback = null)
        {
            int dop = maxDegree ?? Math.Max(1, Environment.ProcessorCount - 1);

            await Parallel.ForEachAsync(
                items,
                new ParallelOptions { MaxDegreeOfParallelism = dop, CancellationToken = cancellationToken },
                async (pair, ct) =>
                {
                    string jobDir = null;
                    try
                    {
                        if (!s_tempDirPool.TryTake(out jobDir))
                        {
                            jobDir = Path.Combine(s_workRoot, SafeName(pair.objPath) + "_" + Guid.NewGuid().ToString("N"));
                            Directory.CreateDirectory(jobDir);
                        }

                        List<Obj.CollisionMaterial> materials = Obj.GetMaterials(pair.objPath);
                        byte[] stage = await ObjToHkxIsolatedAsync(jobDir, pair.objPath, ct);
                        byte[] upgraded = await UpgradeHKXAsync(jobDir, stage, materials, pair.objPath, ct);
                        Directory.CreateDirectory(Path.GetDirectoryName(pair.hkxPath) ?? ".");
                        await File.WriteAllBytesAsync(pair.hkxPath, upgraded, ct);
                    }
                    catch (Exception ex)
                    {
                        Lort.Log($"Failed to process {pair.objPath}: {ex.Message}", Lort.Type.Debug);
                    }
                    finally
                    {
                        progressCallback?.Invoke(); // Call even on failure to advance progress
                        // Clear job dir and return to pool
                        try
                        {
                            if (jobDir != null)
                            {
                                foreach (var file in Directory.GetFiles(jobDir)) File.Delete(file);
                                s_tempDirPool.Add(jobDir);
                            }
                        }
                        catch { }
                    }
                });
        }

        // Isolated temp dir 
        private static async Task<byte[]> ObjToHkxIsolatedAsync(string workDir, string objPath, CancellationToken ct)
        {
            string fName = Path.GetFileNameWithoutExtension(objPath);

            string localObj = Path.Combine(workDir, fName + ".obj");
            string localMtl = Path.Combine(workDir, fName + ".mtl");
            string o2f = Path.Combine(workDir, fName + ".obj.o2f");
            string stage1 = Path.Combine(workDir, fName + ".1");
            string stageHkx = Path.Combine(workDir, fName + ".1.hkx");

            await Task.WhenAll(
                FileCopyAsync(objPath, localObj, true, ct),
                FileCopyAsync(Utility.ResourcePath("misc\\havok.mtl"), localMtl, true, ct));

            var psi = NewPSI(Path.Combine(s_toolDir, "obj2fsnp.exe"), localObj, workDir);
            await RunProcessAsync(psi, ct);

            psi = NewPSI(Path.Combine(s_toolDir, "AssetCc2_fixed.exe"), $"--strip {o2f} {stage1}", workDir);
            await RunProcessAsync(psi, ct);

            psi = NewPSI(Path.Combine(s_toolDir, "hknp2fsnp.exe"), stage1, workDir);
            await RunProcessAsync(psi, ct);

            return await ReadAllBytesFastAsync(stageHkx, ct);
        }

        private static async Task<byte[]> UpgradeHKXAsync(string tempDir, byte[] bytes, List<Obj.CollisionMaterial> materials, string? objPathForLog = null, CancellationToken ct = default)
        {
            var root = (HKX2.hkRootLevelContainer)s_deserializer.Value
                .Deserialize(new BinaryReaderEx(false, bytes));

            hkRootLevelContainer hkx = HkxUpgrader.UpgradehkRootLevelContainer(root);

            List<HKLib.hk2018.fsnpCustomMeshParameter.PrimitiveData> mats =
                ((HKLib.hk2018.fsnpCustomParamCompressedMeshShape)((HKLib.hk2018.hknpPhysicsSceneData)hkx.m_namedVariants[0].m_variant).m_systemDatas[0].m_bodyCinfos[0].m_shape).m_pParam.m_primitiveDataArray;
            if (mats.Count > materials.Count)
            {
                string logName = objPathForLog != null ? Utility.PathToFileName(objPathForLog) + ".obj" : "unknown.obj";
                Lort.Log($"Mismatch in HKX hitmrtl repair: {logName}", Lort.Type.Debug);
            }
            for (int i = 0; i < mats.Count; i++)
            {
                mats[i].m_materialNameData = ((uint)materials[i]);
            }

            int reserve = Math.Max(bytes.Length + (bytes.Length >> 2), 64 * 1024);
            using (var ms = new MemoryStream(reserve))
            {
                // Wrap the MemoryStream so the serializer cannot close it.
                using (var ncs = new NonClosingStream(ms))
                {
                    if (Const.DEBUG_HKX_FORCE_BINARY)
                    {
                        s_binSerializer.Value.Write(hkx, ncs);
                    }
                    else
                    {
                        s_xmlSerializer.Value.Write(hkx, ncs);
                    }
                }

                // Now ms is still open regardless of serializer behavior.
                if (ms.TryGetBuffer(out ArraySegment<byte> seg) && seg.Array != null)
                {
                    byte[] outArr = new byte[ms.Length];
                    Buffer.BlockCopy(seg.Array, 0, outArr, 0, (int)ms.Length);
                    return outArr;
                }
                return ms.ToArray();
            }
        }

        // Small helpers for perf 

        private static ProcessStartInfo NewPSI(string exe, string args, string workingDir)
        {
            return new ProcessStartInfo
            {
                FileName = exe,
                Arguments = args,
                WorkingDirectory = workingDir,
                UseShellExecute = false,
                CreateNoWindow = true,
                WindowStyle = ProcessWindowStyle.Hidden,
                LoadUserProfile = false
            };
        }
        private static async Task<byte[]> ReadAllBytesFastAsync(string path, CancellationToken ct = default)
        {
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 16, FileOptions.SequentialScan | FileOptions.Asynchronous))
            {
                byte[] data = new byte[fs.Length];
                int offset = 0;
                while (offset < data.Length)
                {
                    int read = await fs.ReadAsync(data.AsMemory(offset, data.Length - offset), ct);
                    if (read == 0) break;
                    offset += read;
                }
                return data;
            }
        }
        private static async Task FileCopyAsync(string source, string dest, bool overwrite, CancellationToken ct = default)
        {
            if (source == dest || (File.Exists(dest) && overwrite && File.GetLastWriteTimeUtc(source) <= File.GetLastWriteTimeUtc(dest)))
                return;

            using (var src = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read, 1 << 16, FileOptions.Asynchronous))
            using (var dst = new FileStream(dest, overwrite ? FileMode.Create : FileMode.CreateNew, FileAccess.Write, FileShare.None, 1 << 16, FileOptions.Asynchronous))
            {
                await src.CopyToAsync(dst, ct);
            }
        }
        private static async Task RunProcessAsync(ProcessStartInfo psi, CancellationToken ct = default)
        {
            using (var process = new Process { StartInfo = psi })
            {
                process.Start();
                await process.WaitForExitAsync(ct);
            }
        }
        private static string SafeName(string path)
        {
            string name = Path.GetFileNameWithoutExtension(path);
            foreach (char c in Path.GetInvalidFileNameChars())
                name = name.Replace(c, '_');
            return name;
        }
        // Stream wrapper that forwards calls but ignores Close and Dispose.
        private sealed class NonClosingStream : Stream
        {
            private readonly Stream _inner;

            public NonClosingStream(Stream inner)
            {
                _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            }

            public override bool CanRead => _inner.CanRead;
            public override bool CanSeek => _inner.CanSeek;
            public override bool CanWrite => _inner.CanWrite;
            public override long Length => _inner.Length;
            public override long Position { get => _inner.Position; set => _inner.Position = value; }
            public override void Flush() { _inner.Flush(); }
            public override int Read(byte[] buffer, int offset, int count) { return _inner.Read(buffer, offset, count); }
            public override long Seek(long offset, SeekOrigin origin) { return _inner.Seek(offset, origin); }
            public override void SetLength(long value) { _inner.SetLength(value); }
            public override void Write(byte[] buffer, int offset, int count) { _inner.Write(buffer, offset, count); }
            protected override void Dispose(bool disposing)
            {
                // Do not dispose the inner stream.
            }
            public override void Close()
            {
                // Ignore close to keep the underlying stream usable.
            }
        }
    }
}