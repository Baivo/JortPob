using JortPob.Common;
using SoulsFormats;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using static JortPob.Script;
using static JortPob.Script.Flag;
using static SoulsFormats.MSBAC4.Event;

namespace JortPob
{
    public class ScriptManager
    {
        public ScriptCommon common;
        public List<Script> scripts; // map scripts

        public ScriptManager()
        {
            common = new();
            scripts = new();
        }

        public Script GetScript(int map, int x, int y, int block)
        {
            foreach (Script script in scripts)
            {
                if (script.map == map && script.x == x && script.y == y && script.block == block)
                {
                    return script;
                }
            }

            Script s = new(common, map, x, y, block);
            scripts.Add(s);
            return s;
        }

        public Script GetScript(BaseTile tile)
        {
            if (tile.GetType() != typeof(Tile)) { return null; } // big/huge tiles don't need scripts

            return GetScript(tile.map, tile.coordinate.x, tile.coordinate.y, tile.block);
        }

        public Script GetScript(InteriorGroup group)
        {
            return GetScript(group.map, group.area, group.unk, group.block);
        }

        public Script.Flag GetFlag(Designation designation, string name)
        {
            Script.Flag FindFlag(List<Script.Flag> flags, Designation designation, string name)
            {
                foreach (Script.Flag flag in flags)
                {
                    if(flag.designation == designation && flag.name == name)
                    {
                        return flag;
                    }
                }
                return null;
            }

            Script.Flag f = FindFlag(common.flags, designation, name);
            if(f != null) { return f; }

            foreach (Script script in scripts)
            {
                f = FindFlag(script.flags, designation, name);
                if (f != null) { return f; }
            }

            return null;
        }

        /* Write all EMEVD scripts this class has created */
        public void Write()
        {
            /* Debuggy thing */
            if (Const.DEBUG_SET_ALL_FLAGS_DEFAULT_ON_LOAD)
            {
                List<Flag> allFlags = new();
                allFlags.AddRange(common.flags);
                foreach(Script script in scripts)
                {
                    allFlags.AddRange(script.flags);
                }

                EMEVD.Event init = common.emevd.Events[0];

                foreach (Flag flag in allFlags)
                {
                    if (flag.category == Flag.Category.Event) { continue; } // not values, used for event ids
                    for(int i=0;i<(int)flag.type;i++)
                    {
                        bool bit = (flag.value & (1 << i)) != 0;
                        init.Instructions.Add(common.AUTO.ParseAdd($"SetEventFlag(TargetEventFlagType.EventFlag, {flag.id+i}, {(bit?"ON":"OFF")});"));
                    }
                }
            }

            Lort.Log($"Writing {scripts.Count + 1} EMEVDs...", Lort.Type.Main);
            common.Write();
            foreach(Script script in scripts)
            {
                script.Write();
            }
        }
    }
}
