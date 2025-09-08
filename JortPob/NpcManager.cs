using JortPob.Common;
using JortPob.Worker;
using SoulsFormats;
using System;
using System.Collections.Generic;
using System.Linq;
using static JortPob.Dialog;

namespace JortPob
{
    public class NpcManager
    {
        /* This class is responsible for creating all the data/files needed for NPC dialog */
        /* This includes soundbanks, esd, and fmgs */

        private ESM esm;
        private SoundManager sound;
        private Paramanager param;
        private TextManager text;
        private ScriptManager scriptManager;

        private Dictionary<string, int> topicText; // topic text id map
        private List<EsdInfo> esds;
        private Dictionary<string, int> npcParamMap;

        private int nextNpcParamId;  // increment by 10

        public NpcManager(ESM esm, SoundManager sound, Paramanager param, TextManager text, ScriptManager scriptManager)
        {
            this.esm = esm;
            this.sound = sound;
            this.param = param;
            this.text = text;
            this.scriptManager = scriptManager;

            esds = new();
            npcParamMap = new();
            topicText = new();

            nextNpcParamId = 544900010;
        }

        public int GetParam(NpcContent content)
        {
            // First check if we already generated one for this npc record. If we did return that one. Some npcs like guards and dreamers have multiple placements
            if(npcParamMap.ContainsKey(content.id)) { return npcParamMap[content.id]; }

            int id = nextNpcParamId += 10;
            param.GenerateNpcParam(text, id, content);
            npcParamMap.Add(content.id, id);
            return id;
        }

        /* Returns esd id, creates it if it does't exist */
        /* ESDs are generally 1 to 1 with characters but there are some exceptions like guards */
        // @TODO: THIS SYSTEM USING AN ARRAY OF INTS IS FUCKING SHIT PLEASE GOD REFACTOR THIS TO JUST USE THE ACTUAL TILE OR INTERIOR GROUP JESUS
        public int GetESD(int[] msbIdList, NpcContent content)
        {
            if (Const.DEBUG_SKIP_ESD) { return 0; } // debug skip

            // First check if we even need one, hostile or dead npcs dont' get talk data for now
            if (content.dead || content.hostile) { return 0; }

            // Second check if an esd already exists for the given NPC Record. Return that. This is sort of slimy since a few generaetd values may be incorrect for a given instance of an npc but w/e
            // @TODO: I can basically guarantee this will cause issues in the future. guards are the obvious thing since if every guard shares esd then they will share all values like disposition
            EsdInfo lookup = GetEsdInfo(content.id);
            if (lookup != null) { lookup.AddMsb(msbIdList); return lookup.id; }

            List<Tuple<DialogRecord, List<DialogInfoRecord>>> dialog = esm.GetDialog(content);
            SoundManager.SoundBankInfo bankInfo = sound.GetBank(content);

            List<TopicData> data = new();
            foreach (Tuple<DialogRecord, List<DialogInfoRecord>> tuple in dialog)
            {
                DialogRecord dia = tuple.Item1;
                List<DialogInfoRecord> infos = tuple.Item2;

                int topicId;
                if (dia.type == DialogRecord.Type.Topic)
                {
                    topicId = topicText.ContainsKey(dia.id) ? topicText[dia.id] : -1;
                    if (topicId < 0)
                    {
                        topicId = text.AddTopic(dia.id);
                        topicText.Add(dia.id, topicId);
                    }
                }
                else
                {
                    topicId = 20000000; // generic "talk"
                }

                TopicData topicData = new(dia, topicId);

                foreach (DialogInfoRecord info in infos)
                {
                    /* Search existing soundbanks for the specific dialoginfo we are about to generate. if it exists just yoink it instead of generating a new one */
                    /* If we generate a new talkparam row for every possible line we run out of talkparam rows entirely and the project fails to build */
                    /* This sharing is required, and unfortunately it had to be added in at the end so its not a great implementation */
                    SoundBank.Sound snd = sound.FindSound(content, info.id); // look for a generated wem sound that matches the npc (race/sex) and dialog line (dialoginforecord id)

                    // Make a new sound and talkparam row because no suitable match was found!
                    int talkRowId;
                    if (snd == null) { talkRowId = (int)bankInfo.bank.AddSound(@"sound\test_sound.wav", info.id, info.text); }
                    // Use an existing wem and talkparam we already generated because it's a match
                    else { talkRowId = (int)bankInfo.bank.AddSound(snd); }
                    // The parmanager function will automatically skip duplicates when addign talkparam rows so we don't need to do anything here. the esd gen needs those dupes so ye
                    topicData.talks.Add(new(info, talkRowId));
                }

                if (topicData.talks.Count > 0) { data.Add(topicData); } // if no valid lines for a topic, discard
            }
            param.GenerateTalkParam(text, data);

            int esdId = int.Parse($"{bankInfo.id.ToString("D3")}{bankInfo.uses++.ToString("D2")}6000");  // i know guh guhhhhh

            Script areaScript = scriptManager.GetScript(msbIdList[0], msbIdList[1], msbIdList[2], msbIdList[3]); // get area script for this npc

            DialogESD dialogEsd = new(scriptManager, areaScript, (uint)esdId, content, data);
            string pyPath = $"{Const.CACHE_PATH}esd\\t{esdId}.py";
            string esdPath = $"{Const.CACHE_PATH}esd\\t{esdId}.esd";
            dialogEsd.Write(pyPath);

            EsdInfo esdInfo = new(pyPath, esdPath, content.id, esdId);
            esdInfo.AddMsb(msbIdList);
            esds.Add(esdInfo);

            return esdId;
        }

        /* I dont know what the fuck i was thinking when i wrote this function jesus */
        public void Write()
        {
            EsdWorker.Go(esds);

            Lort.Log($"Binding {esds.Count()} ESDs...", Lort.Type.Main);
            Lort.NewTask($"Binding ESDs", esds.Count());

            Dictionary<int, BND4> bnds = new();
            foreach(EsdInfo esdInfo in esds)
            {
                foreach (int msbId in esdInfo.msbIds)
                {
                    if(!bnds.ContainsKey(msbId))
                    {
                        BND4 nubnd = new();
                        nubnd.Compression = SoulsFormats.DCX.Type.DCX_KRAK;
                        nubnd.Version = "07D7R6";
                        bnds.Add(msbId, nubnd);
                    }
                }
            }

            for (int i = 0; i < esds.Count() ; i++)
            {
                string esdPath = esds[i].esd;

                foreach (int msbId in esds[i].msbIds)
                {
                    ESD esd = ESD.Read(esdPath);

                    BinderFile file = new();
                    file.Bytes = esd.Write();
                    file.Name = $"N:\\GR\\data\\INTERROOT_win64\\script\\talk\\m{msbId.ToString("D4").Substring(0, 2)}_{msbId.ToString("D4").Substring(2, 2)}_00_00\\{Utility.PathToFileName(esdPath)}.esd";
                    file.ID = i;

                    bnds[msbId].Files.Add(file);
                }
                Lort.TaskIterate();
            }

            Lort.Log($"Writing {bnds.Count} Binded ESDs... ", Lort.Type.Main);
            Lort.NewTask($"Writing {bnds.Count} Binded ESDs... ", bnds.Count);
            foreach (KeyValuePair<int, BND4> kvp in bnds)
            {
                BND4 bnd = kvp.Value;
                var files = bnd.Files;
                int n = files.Count;

                if (n > 1)
                {
                    // copy to array for fast sort
                    BinderFile[] arr = files.ToArray();
                    uint[] keys = new uint[n];

                    for (int i = 0; i < n; i++)
                        keys[i] = BinderFileIdComparer.ParseBinderFileId(arr[i]); // fast parse function that avoids Substring if possible

                    Array.Sort(keys, arr); // sorts arr by keys (closest to minimal overhead)

                    // copy back and reassign IDs
                    for (int i = 0; i < n; i++)
                    {
                        files[i] = arr[i];
                        files[i].ID = i;
                    }
                }

                kvp.Value.Write($"{Const.OUTPUT_PATH}script\\talk\\m{kvp.Key.ToString("D4").Substring(0, 2)}_{kvp.Key.ToString("D4").Substring(2, 2)}_00_00.talkesdbnd.dcx");
                Lort.TaskIterate();
            }

            //foreach (KeyValuePair<int, BND4> kvp in bnds)
            //{
            //    /* Sort bnd ?? test */
            //    BND4 bnd = kvp.Value;
            //    for (int i = 0; i < bnd.Files.Count() - 1; i++)
            //    {
            //        BinderFile file = bnd.Files[i];
            //        uint fileId = uint.Parse(Utility.PathToFileName(file.Name).Substring(1));
            //        BinderFile next = bnd.Files[i+1];
            //        uint nextId = uint.Parse(Utility.PathToFileName(next.Name).Substring(1));

            //        if (nextId < fileId)
            //        {
            //            BinderFile temp = file;
            //            bnd.Files[i] = next;
            //            bnd.Files[i + 1] = temp;
            //            i = 0; // slow and bad
            //        }
            //    }

            //    for(int i = 0; i < bnd.Files.Count() ; i++)
            //    {
            //        BinderFile file = bnd.Files[i];
            //        file.ID = i;
            //    }

            //    kvp.Value.Write($"{Const.OUTPUT_PATH}script\\talk\\m{kvp.Key.ToString("D4").Substring(0, 2)}_{kvp.Key.ToString("D4").Substring(2, 2)}_00_00.talkesdbnd.dcx");
            //}
        }

        private EsdInfo GetEsdInfo(string content)
        {
            foreach(EsdInfo esdInfo in esds)
            {
                if (esdInfo.content == content) { return esdInfo; }
            }
            return null;
        }

        public class EsdInfo
        {
            public readonly string py, esd, content;
            public readonly int id;
            public readonly List<int> msbIds;

            public EsdInfo(string py, string esd, string content, int id)
            {
                this.py = py;                                // path to the python source file
                this.esd = esd;        // path to compiled esd
                this.content = content;
                this.id = id;
                msbIds = new();
            }

            public void AddMsb(int[] msbId)
            {
                int[] alteredId;
                if (msbId[0] == 60) { alteredId = new[] { 60, 0 }; }
                else { alteredId = new[] { msbId[0], msbId[1] }; }
                int GUH = (alteredId[0] * 100) + alteredId[1];
                if (!msbIds.Contains(GUH)) { msbIds.Add(GUH); }
            }
        }

        public class TopicData
        {
            public readonly DialogRecord dialog;
            public readonly int topicText;
            public readonly List<TalkData> talks;

            public TopicData(DialogRecord dialog, int topicText)
            {
                this.dialog = dialog;
                this.topicText = topicText;
                this.talks = new();
            }

            public class TalkData
            {
                public readonly DialogInfoRecord dialogInfo;
                public readonly int talkRow;

                public TalkData(DialogInfoRecord dialogInfo, int talkRow)
                {
                    this.dialogInfo = dialogInfo;
                    this.talkRow = talkRow;
                }
            }
        }


    }
}
