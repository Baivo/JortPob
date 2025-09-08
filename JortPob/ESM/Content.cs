using JortPob.Common;
using System;
using System.Numerics;
using System.Text.Json.Nodes;

namespace JortPob
{
    /* Content is effectively any physical object in the game world. Anything that has a physical position in a cell */
    public abstract class Content
    {
        public readonly Cell cell;

        public readonly string id;  // record id
        public readonly ESM.Type type;

        public uint entity;  // entity id, usually 0
        public Vector3 relative;
        public Int2 load; // if a piece of content needs tile load data this is where it's stored

        public readonly Vector3 position;
        public Vector3 rotation;
        public readonly int scale;  // scale in converted to a int where 100 = 1.0f scale. IE:clamp to nearest 1%. this is to group scale for asset generation.

        public string mesh;  // can be null!

        public Content(Cell cell, JsonNode json, Record record)
        {
            this.cell = cell;
            id = json["id"].ToString();

            type = record.type;
            entity = 0;

            float x = float.Parse(json["translation"][0].ToString());
            float z = float.Parse(json["translation"][1].ToString());
            float y = float.Parse(json["translation"][2].ToString());

            float i = float.Parse(json["rotation"][0].ToString());
            float j = float.Parse(json["rotation"][1].ToString());
            float k = float.Parse(json["rotation"][2].ToString());

            /* The following unholy code converts morrowind (Z up) euler rotations into dark souls (Y up) euler rotations */
            /* Big thanks to katalash, dropoff, and the TESUnity dudes for helping me sort this out */

            /* Katalashes code from MapStudio */
            Vector3 MatrixToEulerXZY(Matrix4x4 m)
            {
                const float Pi = (float)Math.PI;
                const float Deg2Rad = Pi / 180.0f;
                Vector3 ret;
                ret.Z = MathF.Asin(-Math.Clamp(-m.M12, -1, 1));

                if (Math.Abs(m.M12) < 0.9999999)
                {
                    ret.X = MathF.Atan2(-m.M32, m.M22);
                    ret.Y = MathF.Atan2(-m.M13, m.M11);
                }
                else
                {
                    ret.X = MathF.Atan2(m.M23, m.M33);
                    ret.Y = 0;
                }
                ret.X = ret.X <= -180.0f * Deg2Rad ? ret.X + 360.0f * Deg2Rad : ret.X;
                ret.Y = ret.Y <= -180.0f * Deg2Rad ? ret.Y + 360.0f * Deg2Rad : ret.Y;
                ret.Z = ret.Z <= -180.0f * Deg2Rad ? ret.Z + 360.0f * Deg2Rad : ret.Z;
                return ret;
            }

            /* Adapted code from https://github.com/ColeDeanShepherd/TESUnity */
            Quaternion xRot = Quaternion.CreateFromAxisAngle(new Vector3(1.0f, 0.0f, 0.0f), i);
            Quaternion yRot = Quaternion.CreateFromAxisAngle(new Vector3(0.0f, 1.0f, 0.0f), k);
            Quaternion zRot = Quaternion.CreateFromAxisAngle(new Vector3(0.0f, 0.0f, 1.0f), j);
            Quaternion q = xRot * zRot * yRot;

            Vector3 eu = MatrixToEulerXZY(Matrix4x4.CreateFromQuaternion(q));

            relative = new();
            position = new Vector3(x, y, z) * Const.GLOBAL_SCALE;
            rotation = eu * (float)(180 / Math.PI);
            scale = (int)((json["scale"] != null ? float.Parse(json["scale"].ToString()) : 1f) * 100);
        }

        public Content(string id, ESM.Type type, Int2 load, Vector3 position, Vector3 rotation, int scale)
        {
            this.id = id;
            this.type = type;
            this.load = load;
            this.position = position;
            this.rotation = rotation;
            this.scale = scale;
        }
    }

    /* npcs, humanoid only */
    public class NpcContent : Content
    {
        public enum Race { Any, Argonian, Khajiit, Imperial, Redguard, Nord, Breton, WoodElf, DarkElf, HighElf, Orc }
        public enum Sex { Any, Male, Female };

        public readonly string name, job, faction; // class is job, cant used reserved word
        public readonly Race race;
        public readonly Sex sex;

        public readonly int level, disposition, reputation, rank, gold;
        public readonly int hello, fight, flee, alarm;
        public readonly bool hostile, dead;

        public readonly bool services; // @TODO: STUB! NEED TO ACTUALLY PARSE AND USE THE INDIVIDUAL SERVICE TYPES

        public NpcContent(Cell cell, JsonNode json, Record record) : base(cell, json, record)
        {
            name = record.json["name"].ToString();
            race = (Race)System.Enum.Parse(typeof(Race), record.json["race"].ToString().Replace(" ", ""));
            job = record.json["class"].ToString();
            faction = record.json["faction"].ToString();

            sex = record.json["npc_flags"].ToString().ToLower().Contains("female") ? Sex.Female : Sex.Male;

            level = int.Parse(record.json["data"]["level"].ToString());
            disposition = int.Parse(record.json["data"]["disposition"].ToString());
            reputation = int.Parse(record.json["data"]["reputation"].ToString());
            rank = int.Parse(record.json["data"]["rank"].ToString());
            gold = int.Parse(record.json["data"]["gold"].ToString());

            hello = int.Parse(record.json["ai_data"]["hello"].ToString());
            fight = int.Parse(record.json["ai_data"]["fight"].ToString());
            flee = int.Parse(record.json["ai_data"]["flee"].ToString());
            alarm = int.Parse(record.json["ai_data"]["alarm"].ToString());

            hostile = fight >= 80; // @TODO: recalc with disposition mods based off UESP calc
            dead = record.json["data"]["stats"] != null && record.json["data"]["stats"]["health"] != null ? (int.Parse(record.json["data"]["stats"]["health"].ToString()) <= 0) : false;

            services = record.json["ai_data"]["services"].ToString().Trim() != "";

            rotation += new Vector3(0f, 180f, 8);  // models are rotated during conversion, placements like this are rotated here during serializiation to match
        }
    }

    /* creatures, both leveled and non-leveled */
    public class CreatureContent : Content
    {
        public CreatureContent(Cell cell, JsonNode json, Record record) : base(cell, json, record)
        {
            // Kinda stubby for now

            rotation += new Vector3(0f, 180f, 8);  // models are rotated during conversion, placements like this are rotated here during serializiation to match
        }
    }

    /* static meshes to be converted to assets */
    public class AssetContent : Content
    {
        public AssetContent(Cell cell, JsonNode json, Record record) : base(cell, json, record)
        {
            mesh = record.json["mesh"].ToString().ToLower();
        }

        public EmitterContent ConvertToEmitter()
        {
            return new EmitterContent(id, type, load, position, rotation, scale, mesh);
        }
    }

    /* doors, both warp doors and activator doors */
    public class DoorContent : Content
    {
        public class Warp
        {
            // this data comes from the esm, we use it to resolve the actual data we will use
            public readonly string cell;
            public readonly Vector3 position, rotation;

            // this is the actual warp data we generate
            public int map, x, y, block;
            public uint entity;

            public Warp(JsonNode json)
            {
                float x = float.Parse(json["translation"][0].ToString());
                float z = float.Parse(json["translation"][1].ToString());
                float y = float.Parse(json["translation"][2].ToString());

                float i = float.Parse(json["rotation"][0].ToString());
                float j = float.Parse(json["rotation"][1].ToString());
                float k = float.Parse(json["rotation"][2].ToString());

                position = new Vector3(x, y, z) * Const.GLOBAL_SCALE;
                rotation = new Vector3(i, j, k) + new Vector3(0f, 180f, 8);  // models are rotated during conversion, placements like this are rotated here during serializing to match
                cell = json["cell"].ToString().Trim();
                if (cell == "") { cell = null; }
            }
        }

        public Warp warp;
        public DoorContent(Cell cell, JsonNode json, Record record) : base(cell, json, record)
        {
            mesh = record.json["mesh"].ToString().ToLower();

            if (json["destination"]  == null) { warp = null; }
            else
            {
                warp = new(json["destination"]);
            }
        }
    }

    /* static meshes that have emitters/lights EX: candles/campfires -- converted to assets but also generates ffx files and params to make them work */
    public class EmitterContent : Content
    {
        public EmitterContent(Cell cell, JsonNode json, Record record) : base(cell, json, record)
        {
            mesh = record.json["mesh"].ToString().ToLower();
        }

        public EmitterContent(string id, ESM.Type type, Int2 load, Vector3 position, Vector3 rotation, int scale, string mesh) : base(id, type, load, position, rotation, scale)
        {
            this.mesh = mesh;
        }
    }

    /* invisible lights with no static mesh associated */
    public class LightContent : Content 
    {
        public readonly Byte4 color;
        public readonly float radius, weight;
        public readonly int value, time;

        public bool dynamic, fire, negative, defaultOff;
        public Mode mode;

        public enum Mode { Flicker, FlickerSlow, Pulse, PulseSlow, Default }

        public LightContent(Cell cell, JsonNode json, Record record) : base(cell, json, record)
        {
            int r = int.Parse(record.json["data"]["color"][0].ToString());
            int g = int.Parse(record.json["data"]["color"][1].ToString());
            int b = int.Parse(record.json["data"]["color"][2].ToString());
            int a = int.Parse(record.json["data"]["color"][3].ToString());
            color = new(r, g, b, a);  // 0 -> 255 colors

            radius = float.Parse(record.json["data"]["radius"].ToString()) * Const.GLOBAL_SCALE;
            weight = float.Parse(record.json["data"]["weight"].ToString());

            value = int.Parse(record.json["data"]["value"].ToString());
            time = int.Parse(record.json["data"]["time"].ToString());

            string flags = record.json["data"]["flags"].ToString();

            dynamic = flags.Contains("DYNAMIC");
            fire = flags.Contains("FIRE");
            negative = flags.Contains("NEGATIVE");
            defaultOff = flags.Contains("OFF_BY_DEFAULT");

            if (flags.Contains("FLICKER_SLOW")) { mode = Mode.FlickerSlow; }
            else if (flags.Contains("FLICKER")) { mode = Mode.Flicker; }
            else if (flags.Contains("PULSE_SLOW")) { mode = Mode.PulseSlow; }
            else if (flags.Contains("PULSE")) { mode = Mode.Pulse; }
            else { mode = Mode.Default; }
        }
    }
}
