using HKLib.hk2018.hkaiCollisionAvoidance.Solver;
using HKLib.hk2018.hke;
using JortPob.Common;
using JortPob.Worker;
using static JortPob.Dialog;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using static JortPob.NpcContent;
using static JortPob.NpcManager.TopicData;
using static JortPob.Script;

namespace JortPob
{
    public class ESM
    {
        /* Types of records in the ESM */
        public enum Type
        {
            Header, GameSetting, GlobalVariable, Class, Faction, Race, Sound, Skill, MagicEffect, Script, Region, Birthsign, LandscapeTexture, Spell, Static, Door,
            MiscItem, Weapon, Container, Creature, Bodypart, Light, Enchantment, Npc, Armor, Clothing, RepairTool, Activator, Apparatus, Lockpick, Probe, Ingredient,
            Book, Alchemy, LevelledItem, LevelledCreature, Cell, Landscape, PathGrid, SoundGen, Dialogue, DialogueInfo
        }

        public Dictionary<Type, List<JsonNode>> records;
        public List<DialogRecord> dialog;
        public List<Cell> exterior, interior;
        private ConcurrentBag<Landscape> landscapes;

        public ESM(string path, ScriptManager scriptManager)
        {
            Lort.Log($"Loading '{path}' ...", Lort.Type.Main);

            string tempRawJson = File.ReadAllText(path);
            JsonArray json = JsonNode.Parse(tempRawJson).AsArray();

            records = new Dictionary<Type, List<JsonNode>>();
            foreach (string name in Enum.GetNames(typeof(Type)))
            {
                Enum.TryParse(name, out Type type);
                if (type == Type.Dialogue || type == Type.DialogueInfo) { continue; } // special records, need to be handled specially
                records.Add(type, new List<JsonNode>());
            }

            for (int i = 0; i < json.Count; i++)
            {
                JsonNode record = json[i];
                foreach (string name in Enum.GetNames(typeof(Type)))
                {
                    if (record["type"].ToString() == name)
                    {
                        Enum.TryParse(name, out Type type);
                        if (type == Type.Dialogue || type == Type.DialogueInfo) { continue; } // special records, need to be handled specially
                        records[type].Add(record);
                    }
                }
            }

            /* Handle dialog stuff now */
            dialog = new();
            DialogRecord current = null;
            for (int i = 0; i < json.Count; i++)
            {
                JsonNode record = json[i];
                Enum.TryParse(record["type"].ToString(), out Type type);

                if (type == Type.Dialogue)
                {
                    string idstr = record["id"].ToString();
                    string typestr = idstr.Replace(" ", "");
                    string diatype = record["dialogue_type"].ToString();
                    typestr = new String(typestr.Where(c => c != '-' && (c < '0' || c > '9')).ToArray());
                    if(!Enum.TryParse(typestr, out DialogRecord.Type dtype)) { dtype = DialogRecord.Type.Topic; }
                    if (diatype.ToLower() == "journal") { dtype = DialogRecord.Type.Journal; }

                    if(current != null && current.type == DialogRecord.Type.Greeting && dtype == DialogRecord.Type.Greeting) { continue; } // skip so we can merge all 9 greeting levels into a single thingy

                    current = new(dtype, idstr, scriptManager.common.CreateFlag(Script.Flag.Category.Saved, Script.Flag.Type.Bit, Script.Flag.Designation.TopicEnabled, idstr));
                    dialog.Add(current);
                }
                else if(type == Type.DialogueInfo)
                {
                    DialogInfoRecord dialogInfoRecord = new(current.type, json[i]);
                    current.infos.Add(dialogInfoRecord);
                }
            }

            /* Post process, looking for topic unlocks */
            foreach (DialogRecord topic in dialog)
            {
                foreach(DialogInfoRecord info in topic.infos)
                {
                    foreach (DialogRecord otherTopic in dialog) {
                        if (info.text.ToLower().Contains(otherTopic.id.ToLower()))
                        {
                            if (topic == otherTopic) { continue; } // prevent self succ
                            info.unlocks.Add(otherTopic);
                        }
                    }
                }
            }

            /* Multi threading to speed this up... */
            List<List<Cell>> cells = CellWorker.Go(this);
            exterior = cells[0];
            interior = cells[1];
            landscapes = new();
        }

        /* List of types that we should search for references */
        // more const values we should move somewhere. @TODO
        public readonly Type[] VALID_CONTENT_TYPES = {
            Type.Static, Type.Container, Type.Light, Type.Sound, Type.Skill, Type.Region, Type.Door, Type.MiscItem, Type.Weapon,  Type.Creature, Type.Bodypart, Type.Npc,
            Type.Armor, Type.Clothing, Type.RepairTool, Type.Activator, Type.Apparatus, Type.Lockpick, Type.Probe, Type.Ingredient, Type.Book, Type.Alchemy, Type.LevelledItem,
            Type.LevelledCreature, Type.PathGrid, Type.SoundGen
        };

        /* References don't contain any explicit 'type' data so... we just gotta go find it lol */
        /* @TODO: well actually i think the 'flags' int value in some records is useed as a 32bit boolean array and that may specify record types possibly. Look into it? */
        public Record FindRecordById(string id)
        {
            foreach (ESM.Type type in VALID_CONTENT_TYPES)
            {
                List<JsonNode> list = records[type];

                foreach(JsonNode record in list)
                {
                    if (record["id"] != null && record["id"].ToString() == id)
                    {
                        return new Record(type, record);
                    }
                }
            }
            return null; // Not found!
        }

        public Cell GetCellByGrid(Int2 position)
        {
            foreach (Cell cell in exterior)
            {
                if (cell.coordinate == position) { return cell; }
            }
            return null;
        }

        public Cell GetCellByName(string name)
        {
            foreach (Cell cell in exterior)
            {
                if (cell.name == name) { return cell; }
            }
            foreach (Cell cell in interior)
            {
                if (cell.name == name) { return cell; }
            }
            return null;
        }

        public Landscape GetLandscape(Int2 coordinate)
        {
            if (GetCellByGrid(coordinate) == null) { return null; } // Performance hack.

            foreach(Landscape landscape in landscapes)
            {
                if (landscape.coordinate == coordinate) { return landscape; }
            }

            foreach (JsonNode json in records[Type.Landscape])
            {
                int x = int.Parse(json["grid"][0].ToString());
                int y = int.Parse(json["grid"][1].ToString());

                if (coordinate.x == x && coordinate.y == y)
                {
                    Landscape landscape = new Landscape(this, coordinate, json);
                    landscapes.Add(landscape);
                    return landscape;
                }
            }
            return null;

        }

        /* Same as above but only retursn a landscape if its already fully loaded. Returns null if its not loaded */
        public Landscape GetLoadedLandscape(Int2 coordinate)
        {
            foreach (Landscape landscape in landscapes)
            {
                if (landscape.coordinate == coordinate) { return landscape; }
            }
            return null;
        }

        /* Load all landscapes, single threaded */
        public void LoadLandscapes()
        {
            Lort.Log($"Processing {exterior.Count} landscapes...", Lort.Type.Main);
            Lort.NewTask("Processing Landscape", exterior.Count);
            foreach (Cell cell in exterior)
            {
                GetLandscape(cell.coordinate);
                Lort.TaskIterate();
            }
        }

        /* Get dialog and character data for building esd */
        public List<Tuple<DialogRecord, List<DialogInfoRecord>>> GetDialog(NpcContent npc)
        {
            List<Tuple<DialogRecord, List<DialogInfoRecord>>> ds = new();  // i am really sorry about this type
            foreach(DialogRecord dialogRecord in dialog)
            {
                if (dialogRecord.type == DialogRecord.Type.Journal) { continue; } // obviously skip these lmao

                // Check if the npc meets requirements for any lines in this topic
                List<DialogInfoRecord> infos = new();
                foreach(DialogInfoRecord info in dialogRecord.infos)
                {
                    // Check if the npc meets all static requirements for this dialog line
                    if (info.speaker != null && info.speaker != npc.id) { continue; }
                    if (info.race != NpcContent.Race.Any && info.race != npc.race) { continue; }
                    if (info.job != null && info.job != npc.job) { continue; }
                    if (info.faction != null && info.faction != npc.faction) { continue; }
                    if (info.rank > npc.rank) { continue; }
                    if (info.cell != null && info.cell != npc.cell.name) { continue; }
                    if (info.sex != NpcContent.Sex.Any && info.sex != npc.sex) { continue; }

                    infos.Add(info);

                    // If this line has no filters it means that anything below it is unreachable, so we just break in that case
                    if (info.filters.Count() <= 0 && info.playerFaction == null && info.playerRank <= 0 && info.disposition <= 0) { break; }
                }

                if (infos.Count() > 0) { ds.Add(new(dialogRecord, infos)); } // discard if no valid lines
            }

            return ds;
        }
    }

    public class Record
    {
        public readonly ESM.Type type;
        public readonly JsonNode json;
        public Record(ESM.Type type, JsonNode json)
        {
            this.type = type;
            this.json = json;
        }
    }
}
