using JortPob.Common;
using SoulsFormats;
using System;
using System.Collections.Generic;

namespace JortPob
{
    public class TextManager
    {
        private enum TextType
        {
            TalkMsg = 1, BloodMsg = 2, MovieSubtitle = 3, NetworkMessage = 31, ActionButtonText = 32, EventTextForTalk = 33, EventTextForMap = 34, GR_MenuText = 200, GR_LineHelp = 201, GR_KeyGuide = 202, GR_System_Message_win64 = 203, GR_Dialogues = 204, LoadingTitle = 205, LoadingText = 206, TutorialTitle = 207, TutorialBody = 208, TextEmbedImageName_win64 = 209, ToS_win64 = 210, TalkMsg_dlc01 = 360, BloodMsg_dlc01 = 361, MovieSubtitle_dlc01 = 362, NetworkMessage_dlc01 = 364, ActionButtonText_dlc01 = 365, EventTextForTalk_dlc01 = 366, EventTextForMap_dlc01 = 367, GR_MenuText_dlc01 = 368, GR_LineHelp_dlc01 = 369, GR_KeyGuide_dlc01 = 370, GR_System_Message_win64_dlc01 = 371, GR_Dialogues_dlc01 = 372, LoadingTitle_dlc01 = 373, LoadingText_dlc01 = 374, TutorialTitle_dlc01 = 375, TutorialBody_dlc01 = 376, TalkMsg_dlc02 = 460, BloodMsg_dlc02 = 461, MovieSubtitle_dlc02 = 462, NetworkMessage_dlc02 = 464, ActionButtonText_dlc02 = 465, EventTextForTalk_dlc02 = 466, EventTextForMap_dlc02 = 467, GR_MenuText_dlc02 = 468, GR_LineHelp_dlc02 = 469, GR_KeyGuide_dlc02 = 470, GR_System_Message_win64_dlc02 = 471, GR_Dialogues_dlc02 = 472, LoadingTitle_dlc02 = 473, LoadingText_dlc02 = 474, TutorialTitle_dlc02 = 475, TutorialBody_dlc02 = 476,
            GoodsName = 10, WeaponName = 11, ProtectorName = 12, AccessoryName = 13, MagicName = 14, NpcName = 18, PlaceName = 19, GoodsInfo = 20, WeaponInfo = 21, ProtectorInfo = 22, AccessoryInfo = 23, GoodsCaption = 24, WeaponCaption = 25, ProtectorCaption = 26, AccessoryCaption = 27, MagicInfo = 28, MagicCaption = 29, GemName = 35, GemInfo = 36, GemCaption = 37, GoodsDialog = 41, ArtsName = 42, ArtsCaption = 43, WeaponEffect = 44, GemEffect = 45, GoodsInfo2 = 46, WeaponName_dlc01 = 310, WeaponInfo_dlc01 = 311, WeaponCaption_dlc01 = 312, ProtectorName_dlc01 = 313, ProtectorInfo_dlc01 = 314, ProtectorCaption_dlc01 = 315, AccessoryName_dlc01 = 316, AccessoryInfo_dlc01 = 317, AccessoryCaption_dlc01 = 318, GoodsName_dlc01 = 319, GoodsInfo_dlc01 = 320, GoodsCaption_dlc01 = 321, GemName_dlc01 = 322, GemInfo_dlc01 = 323, GemCaption_dlc01 = 324, MagicName_dlc01 = 325, MagicInfo_dlc01 = 326, MagicCaption_dlc01 = 327, NpcName_dlc01 = 328, PlaceName_dlc01 = 329, GoodsDialog_dlc01 = 330, ArtsName_dlc01 = 331, ArtsCaption_dlc01 = 332, WeaponEffect_dlc01 = 333, GemEffect_dlc01 = 334, GoodsInfo2_dlc01 = 335, WeaponName_dlc02 = 410, WeaponInfo_dlc02 = 411, WeaponCaption_dlc02 = 412, ProtectorName_dlc02 = 413, ProtectorInfo_dlc02 = 414, ProtectorCaption_dlc02 = 415, AccessoryName_dlc02 = 416, AccessoryInfo_dlc02 = 417, AccessoryCaption_dlc02 = 418, GoodsName_dlc02 = 419, GoodsInfo_dlc02 = 420, GoodsCaption_dlc02 = 421, GemName_dlc02 = 422, GemInfo_dlc02 = 423, GemCaption_dlc02 = 424, MagicName_dlc02 = 425, MagicInfo_dlc02 = 426, MagicCaption_dlc02 = 427, NpcName_dlc02 = 428, PlaceName_dlc02 = 429, GoodsDialog_dlc02 = 430, ArtsName_dlc02 = 431, ArtsCaption_dlc02 = 432, WeaponEffect_dlc02 = 433, GemEffect_dlc02 = 434, GoodsInfo2_dlc02 = 435
        }

        private Dictionary<TextType, FMG> menu, item;

        private int nextTopicId, nextNpcNameId, nextActionButtonId, nextLocationId;

        public TextManager()
        {
            nextTopicId = 29000000;
            nextNpcNameId = 11800000;
            nextActionButtonId = 10000;
            nextLocationId = 11000000;

            Dictionary<TextType, FMG> LoadMsgBnd(string path)
            {
                BND4 bnd = BND4.Read(path);
                Dictionary<TextType, FMG> fmgs = new();

                foreach (BinderFile file in bnd.Files)
                {
                    FMG fmg = FMG.Read(file.Bytes);
                    string name = Utility.PathToFileName(file.Name);

                    TextType type = (TextType)Enum.Parse(typeof(TextType), name);

                    fmgs.Add(type, fmg);
                }

                return fmgs;
            }

            menu = LoadMsgBnd(Utility.ResourcePath(@"text\menu_dlc02.msgbnd.dcx"));
            item = LoadMsgBnd(Utility.ResourcePath(@"text\item_dlc02.msgbnd.dcx"));
        }

        private FMG.Entry GetEntry(FMG fmg, int id)
        {
            foreach (FMG.Entry entry in fmg.Entries)
            {
                if (entry.ID == id) { return entry; }
            }
            return null;
        }

        public void AddTalk(int id, string text)
        {
            FMG fmg = menu[TextType.TalkMsg];
            fmg.Entries.Add(new(id, text));
        }

        public int AddTopic(string text)
        {
            int id = nextTopicId++;
            FMG fmg = menu[TextType.EventTextForTalk];
            fmg.Entries.Add(new(id, text));
            return id;
        }

        public int AddNpcName(string text)
        {
            int id = nextNpcNameId++;
            FMG fmg = item[TextType.NpcName];
            fmg.Entries.Add(new(id, text));
            return id;
        }

        public int AddActionButton(string text)
        {
            int id = nextActionButtonId++;
            FMG fmg = menu[TextType.ActionButtonText];
            fmg.Entries.Add(new(id, text));
            return id;
        }

        public void SetLocation(int id, string text)
        {
            FMG fmg = item[TextType.PlaceName];
            FMG.Entry entry = GetEntry(fmg, id);
            if (fmg != null) { fmg.Entries.Remove(entry); }
            fmg.Entries.Add(new(id, text));
        }

        public int AddLocation(string text)
        {
            int id = nextLocationId++;
            FMG fmg = item[TextType.PlaceName];
            fmg.Entries.Add(new(id, text));
            return id;
        }

        public void Write(string dir)
        {

            void WriteBnd(string fileName, Dictionary<TextType, FMG> fmgs)
            {
                BND4 bnd = new();
                bnd.Compression = SoulsFormats.DCX.Type.DCX_KRAK;
                bnd.Version = "07D7R6";


                foreach (KeyValuePair<TextType, FMG> kvp in fmgs)
                {
                    FMG fmg = kvp.Value;
                    if (Const.DEBUG_ENABLE_FMG_PARAM_SORTING)
                    {
                        Utility.SortFMG(fmg);  // it's too slow!!!!!! jesus fucking christ make a faster sort @TODO: !!
                    }

                    BinderFile file = new();
                    file.Name = $"N:\\GR\\data\\INTERROOT_win64\\msg\\engUS\\{kvp.Key.ToString()}.fmg";
                    file.ID = (int)kvp.Key;
                    file.Bytes = fmg.Write();

                    bnd.Files.Add(file);
                }

                bnd.Write($"{dir}{fileName}");
            }

            WriteBnd("menu_dlc02.msgbnd.dcx", menu);
            WriteBnd("item_dlc02.msgbnd.dcx", item);
        }
    }
}
