using JortPob.Common;
using SoulsFormats;
using SoulsIds;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static JortPob.Script;

namespace JortPob
{
    /* Handles CommonEvent and CommonFunc EMEVD. These are different from map scripts so I decided to give them a seperate class */

    public class ScriptCommon
    {
        public Events AUTO;

        public readonly EMEVD emevd, func;

        public List<Flag> flags;
        private Dictionary<Flag.Category, uint> flagUsedCounts;

        public ScriptCommon()
        {
            AUTO = new(Utility.ResourcePath(@"script\\er-common.emedf.json"), true, true);

            emevd = EMEVD.Read(Utility.ResourcePath(@"script\common.emevd.dcx"));
            func = EMEVD.Read(Utility.ResourcePath(@"script\common_func.emevd.dcx"));

            flags = new();

            flagUsedCounts = new()
            {
                { Flag.Category.Event, 0 },
                { Flag.Category.Saved, 0 },
                { Flag.Category.Temporary, 0 }
            };
        }

        /* There are some bugs with this system. It defo wastes some flag space. We have lots tho. Maybe fix later */
        private static readonly uint[] COMMON_FLAG_BASES = new uint[]  // using flags from every msb slot along the bottom most edge of the world
        {
            1030290000, 1031290000, 1032290000, 1033290000 // if we run out of flag space it will throw an exception. adding more is easy tho
        };
        private static readonly Dictionary<Flag.Category, uint[]> FLAG_TYPE_OFFSETS = new()
        {
            { Flag.Category.Event, new uint[] { 1000, 3000, 6000 } },
            { Flag.Category.Saved, new uint[] { 0, 4000, 7000, 8000, 9000 } },
            { Flag.Category.Temporary, new uint[] { 2000, 5000 } }
        };
        public Flag CreateFlag(Flag.Category category, Flag.Type type, Flag.Designation designation, string name, uint value = 0)
        {
            /* Cap off a group of 1000 flags if it's near full. For example: This is to prevent us adding a multi bit flag like a byte when there is only 3 flags left */
            uint rawCount = flagUsedCounts[category];
            if ((rawCount % 1000) + ((uint)type) >= 1000)
            {
                flagUsedCounts[category] += 1000 - (rawCount % 1000);
                rawCount = flagUsedCounts[category];
            }

            /* Calculate next flag */
            uint perThou = (rawCount / 1000) % (uint)(FLAG_TYPE_OFFSETS[category].Length);
            uint perMsb = (rawCount / 1000) / (uint)(FLAG_TYPE_OFFSETS[category].Length);
            uint mod = rawCount % 1000;
            uint mapOffset = COMMON_FLAG_BASES[perMsb];
            uint id = mapOffset + FLAG_TYPE_OFFSETS[category][perThou] + mod;

            Flag flag = new(category, type, designation, name, id, value);
            flags.Add(flag);

            flagUsedCounts[category] += ((uint)type);

            return flag;
        }

        public void Write()
        {
            emevd.Write($"{Const.OUTPUT_PATH}\\event\\common.emevd.dcx");
            func.Write($"{Const.OUTPUT_PATH}\\event\\common_func.emevd.dcx");
        }
    }
}
