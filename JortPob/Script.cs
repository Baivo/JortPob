using HKLib.hk2018.hk;
using HKX2;
using JortPob.Common;
using SoulsFormats;
using SoulsIds;
using System;
using System.Collections.Generic;
using System.Reflection.Metadata.Ecma335;

/* Individual script for an msb. */
/* managed by ScriptManager 
/* When using the word "entity" in this code i am refering to entity id. i just like shorter names */

/* Using this research as a base for conventions here https://docs.google.com/spreadsheets/d/17sE1a1h87BhpiUwKUyJ9ZjKTeehXA4OuLwmQvTfwo_M/edit?gid=1770617590#gid=1770617590 */

namespace JortPob
{
    public class Script
    {
        public Events AUTO;

        public readonly int map, x, y, block;

        public readonly ScriptCommon common; // commonevent and commonfunc emevds
        public readonly EMEVD emevd;
        public readonly EMEVD.Event init;

        public enum EntityType
        {
            Enemy = 0, Asset = 1000, Region = 2000, Event = 3000, Collision = 4000, Group = 5000
        }

        public List<Flag> flags;
        private Dictionary<Flag.Category, uint> flagUsedCounts;
        private Dictionary<EntityType, uint> entityUsedCounts;

        public enum Event
        {
            LoadDoor
        }
        public Dictionary<Event, uint> events;

        public Script(ScriptCommon common, int map, int x, int y, int block)
        {
            this.common = common;
            this.map = map;
            this.x = x;
            this.y = y;
            this.block = block;

            AUTO = new(Utility.ResourcePath(@"script\\er-common.emedf.json"), true, true);

            emevd = new EMEVD();
            emevd.Compression = SoulsFormats.DCX.Type.DCX_KRAK;
            emevd.Format = SoulsFormats.EMEVD.Game.Sekiro;

            init = new EMEVD.Event(0);
            emevd.Events.Add(init);

            flags = new();

            flagUsedCounts = new()
            {
                { Flag.Category.Event, 0 },
                { Flag.Category.Saved, 0 },
                { Flag.Category.Temporary, 0 }
            };

            entityUsedCounts = new()
            {
                { EntityType.Enemy, 0 },
                { EntityType.Asset, 0 },
                { EntityType.Region, 0 },
                { EntityType.Event, 0 },
                { EntityType.Collision, 0 },
                { EntityType.Group, 0 }
            };

            events = new();

            /* Create an event for going through load doors */
            Flag eventFlag = CreateFlag(Flag.Category.Event, Flag.Type.Bit, Flag.Designation.Event, $"m{map}_{x}_{y}_{block}::DoorLoad");
            EMEVD.Event loadDoor = new(eventFlag.id);

            int pc = 0;
            string NextParameterName()
            {
                return $"X{pc++ * 4}_4";
            }

            string[] loadDoorEventRaw = new string[]
            {
                $"IfActionButtonInArea(MAIN, {NextParameterName()}, {NextParameterName()});",
                $"RotateCharacter(10000, {NextParameterName()}, 60000, false);",
                $"WaitFixedTimeSeconds(0.25);",
                $"PlaySE({NextParameterName()}, SoundType.Asset, 200);",
                $"WaitFixedTimeSeconds(0.75);",
                $"WarpPlayer({NextParameterName()}, {NextParameterName()}, {NextParameterName()}, {NextParameterName()}, {NextParameterName()}, -1);",
                $"EndUnconditionally(EventEndType.End);"
            };

            for (int i = 0; i < loadDoorEventRaw.Length; i++)
            {
                (EMEVD.Instruction instr, List<EMEVD.Parameter> newPs) = AUTO.ParseAddArg(loadDoorEventRaw[i], i);
                loadDoor.Parameters.AddRange(newPs);
                loadDoor.Instructions.Add(instr);
            }

            emevd.Events.Add(loadDoor);
            events.Add(Event.LoadDoor, eventFlag.id);
        }

        public void RegisterLoadDoor(DoorContent door)
        {
            int actionParam = door.warp.map == 60 ? 1501 : 1500;  // enter or exit
            init.Instructions.Add(AUTO.ParseAdd($"InitializeCommonEvent(0, {events[Event.LoadDoor]}, {actionParam}, {door.entity}, {door.entity}, {1000}, {door.warp.map}, {door.warp.x}, {door.warp.y}, {door.warp.block}, {door.warp.entity});"));
        }

        private static readonly Dictionary<Flag.Category, uint[]> FLAG_TYPE_OFFSETS = new()
        {
            { Flag.Category.Event, new uint[] { 1000, 3000, 6000 } },
            { Flag.Category.Saved, new uint[] { 0, 4000, 7000, 8000, 9000 } },
            { Flag.Category.Temporary, new uint[] { 2000, 5000 } }
        };
        public Flag CreateFlag(Flag.Category category, Flag.Type type, Flag.Designation designation, string name, uint value = 0)
        {
            uint rawCount = flagUsedCounts[category];
            uint perThou = rawCount / 1000;
            uint mod = rawCount % 1000;
            uint mapOffset;
            if(map == 60) { mapOffset = uint.Parse($"10{x:D2}{y:D2}0000"); }
            else { mapOffset = uint.Parse($"{map:D2}{x:D2}0000"); }

            uint id = mapOffset + FLAG_TYPE_OFFSETS[category][perThou] + mod;  // if we run out of flags this will throw an out of bounds exception. that situation would be bad but should't happen.

            Flag flag = new(category, type, designation, name, id, value);
            flags.Add(flag);

            flagUsedCounts[category] += ((uint)type);

            return flag;
        }

        public uint CreateEntity(EntityType type)
        {
            uint rawCount = entityUsedCounts[type]++;
            uint mapOffset = uint.Parse($"{(map == 60 ? "10" : "")}{x:D2}{y:D2}0000");

            if (rawCount >= 1000) { Lort.Log($" ## CRITICAL ## ENTITY ID OVERFLOWED IN m{map:D2}_{x:D2}_{y:D2}", Lort.Type.Debug); }

            return mapOffset + ((uint)type) + rawCount;
        }

        public void Write()
        {
            emevd.Write($"{Const.OUTPUT_PATH}\\event\\m{map:D2}_{x:D2}_{y:D2}_{block:D2}.emevd.dcx");
        }

        public class Flag
        {
            public enum Category
            {
                Event, Saved, Temporary
            }

            public enum Type
            {
                Bit = 1, Nibble = 4, Byte = 8, Short = 16, Int = 32
            }

            public enum Designation
            {
                Event, CharacterDead, Disabled, Global, TopicEnabled, Journal, TalkedToPc, Disposition
            }

            public readonly Category category;
            public readonly Type type;
            public readonly Designation designation;
            public readonly string name;  // general purpose string to identify this flag. for example, if this is a papyrus global variable, it would be that variables name
            public readonly uint id, value;   // id is flag, value is the default initial value. usually 0

            public Flag(Category category, Type type, Designation designation, string name, uint id, uint value)
            {
                this.category = category;
                this.type = type;
                this.designation = designation;
                this.name = name;
                this.id = id;
                this.value = value;
            }
        }
    }
}
