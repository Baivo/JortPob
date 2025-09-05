using JortPob.Common;
using JortPob.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace JortPob.Worker
{
    public class HkxWorker : Worker
    {
        private readonly List<CollisionInfo> _collisions;
        private readonly int _start;
        private readonly int _end;

        public HkxWorker(List<CollisionInfo> collisions, int start, int end)
        {
            _collisions = collisions;
            _start = start;
            _end = end;

            // Wrap async Run in sync delegate to match Thread type expected by Worker
            _thread = new Thread(() => Run().GetAwaiter().GetResult());
            _thread.Start();
        }

        private async Task Run()
        {
            ExitCode = 1;

            try
            {
                int sliceStart = Math.Max(0, _start);
                int sliceEnd = Math.Min(_collisions.Count, _end);
                if (sliceStart >= sliceEnd)
                {
                    IsDone = true;
                    ExitCode = 0;
                    return;
                }

                // Defer materialization and filter invalid files
                var items = _collisions
                    .Skip(sliceStart)
                    .Take(sliceEnd - sliceStart)
                    .Select(ci => (objPath: $"{Const.CACHE_PATH}{ci.obj}", hkxPath: $"{Const.CACHE_PATH}{ci.hkx}"))
                    .Where(item => File.Exists(item.objPath)); // Skip non-existent OBJ files

                int dop = Math.Max(1, Const.THREAD_COUNT); // Use full DOP unless multiple workers
                await ModelConverter.OBJtoHKXBatch(items, maxDegree: dop, progressCallback: Lort.TaskIterate);

                ExitCode = 0;
            }
            catch (Exception ex)
            {
                Lort.Log($"HKX conversion worker failed: {ex.Message}", Lort.Type.Main);
                ExitCode = -1;
            }
            finally
            {
                IsDone = true;
            }
        }

        public static async Task Go(List<CollisionInfo> collisions)
        {
            Lort.Log($"Converting {collisions.Count} collision...", Lort.Type.Main);
            Lort.NewTask("Converting HKX", collisions.Count);

            // Defer materialization and filter invalid files
            var items = collisions
                .Select(ci => (objPath: $"{Const.CACHE_PATH}{ci.obj}", hkxPath: $"{Const.CACHE_PATH}{ci.hkx}"))
                .Where(item => File.Exists(item.objPath)); // Skip non-existent OBJ files

            int dop = Math.Max(1, Const.THREAD_COUNT);

            ThreadPool.GetMinThreads(out int minWorker, out int minIOC);
            if (minWorker < dop)
                ThreadPool.SetMinThreads(dop, minIOC);

            // Process all items in one batch for maximum parallelism
            await ModelConverter.OBJtoHKXBatch(items, maxDegree: dop, progressCallback: Lort.TaskIterate);
        }
    }
}
