using System.Collections.Generic;
using System.Collections.Specialized;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.IO;
using System.Linq;
using static JortPob.NpcManager.TopicData;
using static SoulsFormats.DRB.Shape;
using static SoulsFormats.MSBS.Event;
using static JortPob.Dialog;

namespace JortPob
{
    /* Handles python state machine code generation for a dialog ESD */
    public class DialogESD
    {
        private ScriptManager scriptManager;
        private Script areaScript;
        private NpcContent npcContent;

        private List<string> defs = new();

        public DialogESD(ScriptManager scriptManager, Script areaScript, uint id, NpcContent npcContent, List<NpcManager.TopicData> topicData)
        {
            this.scriptManager = scriptManager;
            this.areaScript = areaScript;
            this.npcContent = npcContent;

            // Create flags for this character's disposition and first greeting
            Script.Flag firstGreet = areaScript.CreateFlag(Script.Flag.Category.Saved, Script.Flag.Type.Bit, Script.Flag.Designation.TalkedToPc, npcContent.id);
            Script.Flag disposition = areaScript.CreateFlag(Script.Flag.Category.Saved, Script.Flag.Type.Byte, Script.Flag.Designation.Disposition, npcContent.id, (uint)npcContent.disposition);

            // Split up talk data by type
            NpcManager.TopicData greeting = GetTalk(topicData, DialogRecord.Type.Greeting)[0];
            NpcManager.TopicData hit = GetTalk(topicData, DialogRecord.Type.Hit)[0];
            NpcManager.TopicData attack = GetTalk(topicData, DialogRecord.Type.Attack)[0];
            List<NpcManager.TopicData> talk = GetTalk(topicData, DialogRecord.Type.Topic);

            defs.Add($"# dialog esd : {npcContent.id}\r\n");

            defs.Add(State_1(id));

            defs.Add(State_1000(id));
            defs.Add(State_1001(id));
            defs.Add(State_1101(id));
            defs.Add(State_1102(id));
            defs.Add(State_1103(id));
            defs.Add(State_2000(id));

            defs.Add(State_x0(id));
            defs.Add(State_x1(id));
            defs.Add(State_x2(id));
            defs.Add(State_x3(id));
            defs.Add(State_x4(id));
            defs.Add(State_x5(id));
            defs.Add(State_x6(id));
            defs.Add(State_x7(id));
            defs.Add(State_x8(id));
            defs.Add(State_x9(id));

            defs.Add(State_x10(id));
            defs.Add(State_x11(id));
            defs.Add(State_x12(id));
            defs.Add(State_x13(id));
            defs.Add(State_x14(id));
            defs.Add(State_x15(id));
            defs.Add(State_x16(id));
            defs.Add(State_x17(id));
            defs.Add(State_x18(id));
            defs.Add(State_x19(id));

            defs.Add(State_x20(id));
            defs.Add(State_x21(id));
            defs.Add(State_x22(id));
            defs.Add(State_x23(id));
            defs.Add(State_x24(id));
            defs.Add(State_x25(id));
            defs.Add(State_x26(id));
            defs.Add(State_x27(id));
            defs.Add(State_x28(id));
            defs.Add(State_x29(id, greeting));

            defs.Add(State_x30(id));
            defs.Add(State_x31(id));
            defs.Add(State_x32(id));
            defs.Add(State_x33(id));
            defs.Add(State_x34(id));
            defs.Add(State_x35(id));
            defs.Add(State_x36(id));
            defs.Add(State_x37(id));
            defs.Add(State_x38(id, hit.talks[0].talkRow));
            defs.Add(State_x39(id, hit.talks[0].talkRow));

            defs.Add(State_x40(id, attack.talks[0].talkRow));
            defs.Add(State_x41(id, hit.talks[0].talkRow));
            defs.Add(State_x42(id));
            defs.Add(State_x43(id));
            defs.Add(State_x44(id, npcContent.services, talk));
        }

        /* Returns all topics that match the given type. */
        private List<NpcManager.TopicData> GetTalk(List<NpcManager.TopicData> topicData, DialogRecord.Type type)
        {
            List<NpcManager.TopicData> matches = new();
            foreach(NpcManager.TopicData topic in topicData)
            {
                if (topic.dialog.type == type) { matches.Add(topic); }
            }

            return matches;
        }

        public void Write(string pyPath)
        {
            if (!Directory.Exists(Path.GetDirectoryName(pyPath))) { Directory.CreateDirectory(Path.GetDirectoryName(pyPath)); }
            System.IO.File.WriteAllLines(pyPath, defs);
        }



        /* WARNING: SHITCODE BELOW! YOU WERE WARNED! */



        /* Starting state, top level */
        private string State_1(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_1():\r\n    \"\"\"State 0,1\"\"\"\r\n    # actionbutton:6000:\"Talk\"\r\n    t{id_s}_x5(flag6=4743, flag7=4741, flag8=4742, val1=5, val2=10, val3=12, val4=10, val5=12, actionbutton1=6000,\r\n                  flag9=6000, flag10=6001, flag11=6000, flag12=6000, flag13=6000, z1=1, z2=1000000, z3=1000000,\r\n                  z4=1000000, mode1=1, mode2=1)\r\n    Quit()\r\n";
        }

        private string State_1000(uint id)
        {
            string id_s = id.ToString("D9");
            int shop1 = 100625;
            int shop2 = 100649;
            return $"def t{id_s}_1000():\r\n    \"\"\"State 0,2,3\"\"\"\r\n    assert t{id_s}_x37(shop1={shop1}, shop2={shop2})\r\n    \"\"\"State 1\"\"\"\r\n    EndMachine(1000)\r\n    Quit()\r\n";
        }

        private string State_1001(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_1001():\r\n    \"\"\"State 0,2,3\"\"\"\r\n    assert t{id_s}_x38()\r\n    \"\"\"State 1\"\"\"\r\n    EndMachine(1001)\r\n    Quit()\r\n";
        }

        private string State_1101(uint id)
        {
            string id_s = id.ToString("D9");
            int hurtFlag = 1043332705;
            return $"def t{id_s}_1101():\r\n    \"\"\"State 0,2\"\"\"\r\n    assert t{id_s}_x39(flag5={hurtFlag})\r\n    \"\"\"State 1\"\"\"\r\n    EndMachine(1101)\r\n    Quit()\r\n";
        }

        private string State_1102(uint id)
        {
            string id_s = id.ToString("D9");
            int hostileFlag = 1043339205;
            return $"def t{id_s}_1102():\r\n    \"\"\"State 0,2\"\"\"\r\n    t{id_s}_x40(flag4={hostileFlag})\r\n    Quit()\r\n";
        }

        private string State_1103(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_1103():\r\n    \"\"\"State 0,2\"\"\"\r\n    assert t{id_s}_x41()\r\n    \"\"\"State 1\"\"\"\r\n    EndMachine(1103)\r\n    Quit()\r\n";
        }

        private string State_2000(uint id)
        {
            string id_s = id.ToString("D9");
            int unk0Flag = 1043332706;
            int unk1Flag = 1043332707;
            return $"def t{id_s}_2000():\r\n    \"\"\"State 0,2,3\"\"\"\r\n    assert t{id_s}_x42(flag2={unk0Flag}, flag3={unk1Flag})\r\n    \"\"\"State 1\"\"\"\r\n    EndMachine(2000)\r\n    Quit()\r\n";
        }

        private string State_x0(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x0(actionbutton1=6000, flag10=6001, flag14=6000, flag15=6000, flag16=6000, flag17=6000, flag9=6000):\r\n    \"\"\"State 0\"\"\"\r\n    while True:\r\n        \"\"\"State 1\"\"\"\r\n        assert not GetOneLineHelpStatus() and not IsClientPlayer() and not IsPlayerDead() and not IsCharacterDisabled()\r\n        \"\"\"State 3\"\"\"\r\n        assert (GetEventFlag(flag10) or GetEventFlag(flag14) or GetEventFlag(flag15) or GetEventFlag(flag16) or\r\n                GetEventFlag(flag17))\r\n        \"\"\"State 4\"\"\"\r\n        assert not GetEventFlag(flag9)\r\n        \"\"\"State 2\"\"\"\r\n        if (GetEventFlag(flag9) or not (not GetOneLineHelpStatus() and not IsClientPlayer() and not IsPlayerDead()\r\n            and not IsCharacterDisabled()) or (not GetEventFlag(flag10) and not GetEventFlag(flag14) and not GetEventFlag(flag15)\r\n            and not GetEventFlag(flag16) and not GetEventFlag(flag17))):\r\n            pass\r\n        # actionbutton:6000:\"Talk\"\r\n        elif CheckActionButtonArea(actionbutton1):\r\n            break\r\n    \"\"\"State 5\"\"\"\r\n    return 0\r\n";
        }

        private string State_x1(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x1():\r\n    \"\"\"State 0,1\"\"\"\r\n    if not CheckSpecificPersonTalkHasEnded(0):\r\n        \"\"\"State 7\"\"\"\r\n        ClearTalkProgressData()\r\n        StopEventAnimWithoutForcingConversationEnd(0)\r\n        \"\"\"State 6\"\"\"\r\n        ReportConversationEndToHavokBehavior()\r\n    else:\r\n        pass\r\n    \"\"\"State 2\"\"\"\r\n    if CheckSpecificPersonGenericDialogIsOpen(0):\r\n        \"\"\"State 3\"\"\"\r\n        ForceCloseGenericDialog()\r\n    else:\r\n        pass\r\n    \"\"\"State 4\"\"\"\r\n    if CheckSpecificPersonMenuIsOpen(-1, 0) and not CheckSpecificPersonGenericDialogIsOpen(0):\r\n        \"\"\"State 5\"\"\"\r\n        ForceCloseMenu()\r\n    else:\r\n        pass\r\n    \"\"\"State 8\"\"\"\r\n    return 0\r\n";
        }

        private string State_x2(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x2():\r\n    \"\"\"State 0,1\"\"\"\r\n    ClearTalkProgressData()\r\n    StopEventAnimWithoutForcingConversationEnd(0)\r\n    ForceCloseGenericDialog()\r\n    ForceCloseMenu()\r\n    ReportConversationEndToHavokBehavior()\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x3(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x3(val2=10, val3=12):\r\n    \"\"\"State 0,1\"\"\"\r\n    assert GetDistanceToPlayer() < val2 and GetCurrentStateElapsedFrames() > 1\r\n    \"\"\"State 2\"\"\"\r\n    if PlayerDiedFromFallInstantly() == False and PlayerDiedFromFallDamage() == False:\r\n        \"\"\"State 3,6\"\"\"\r\n        call = t{id_s}_x19()\r\n        if call.Done():\r\n            pass\r\n        elif GetDistanceToPlayer() > val3 or GetTalkInterruptReason() == 6:\r\n            \"\"\"State 5\"\"\"\r\n            assert t{id_s}_x1()\r\n    else:\r\n        \"\"\"State 4,7\"\"\"\r\n        call = t{id_s}_x32()\r\n        if call.Done():\r\n            pass\r\n        elif GetDistanceToPlayer() > val3 or GetTalkInterruptReason() == 6:\r\n            \"\"\"State 8\"\"\"\r\n            assert t{id_s}_x1()\r\n    \"\"\"State 9\"\"\"\r\n    return 0\r\n";
        }

        private string State_x4(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x4():\r\n    \"\"\"State 0,1\"\"\"\r\n    assert t{id_s}_x1()\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x5(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x5(flag6=4743, flag7=4741, flag8=4742, val1=5, val2=10, val3=12, val4=10, val5=12, actionbutton1=6000,\r\n                  flag9=6000, flag10=6001, flag11=6000, flag12=6000, flag13=6000, z1=1, z2=1000000, z3=1000000,\r\n                  z4=1000000, mode1=1, mode2=1):\r\n    \"\"\"State 0\"\"\"\r\n    assert GetCurrentStateElapsedTime() > 1.5\r\n    while True:\r\n        \"\"\"State 2\"\"\"\r\n        call = t{id_s}_x22(flag6=flag6, flag7=flag7, flag8=flag8, val1=val1, val2=val2, val3=val3, val4=val4,\r\n                              val5=val5, actionbutton1=actionbutton1, flag9=flag9, flag10=flag10, flag11=flag11,\r\n                              flag12=flag12, flag13=flag13, z1=z1, z2=z2, z3=z3, z4=z4, mode1=mode1, mode2=mode2)\r\n        assert IsClientPlayer()\r\n        \"\"\"State 1\"\"\"\r\n        call = t{id_s}_x21()\r\n        assert not IsClientPlayer()\r\n";
        }

        private string State_x6(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x6(val1=5, val2=10, val3=12, val4=10, val5=12, actionbutton1=6000, flag9=6000, flag10=6001, flag11=6000,\r\n                  flag12=6000, flag13=6000, z1=1, z2=1000000, z3=1000000, z4=1000000, mode1=1, mode2=1):\r\n    \"\"\"State 0\"\"\"\r\n    while True:\r\n        \"\"\"State 2\"\"\"\r\n        call = t{id_s}_x9(actionbutton1=actionbutton1, flag9=flag9, flag10=flag10, z2=z2, z3=z3, z4=z4)\r\n        def WhilePaused():\r\n            RemoveMyAggroIf(IsAttackedBySomeone() and (DoesSelfHaveSpEffect(9626) and DoesSelfHaveSpEffect(9627)))\r\n            GiveSpEffectToPlayerIf(not CheckSpecificPersonTalkHasEnded(0), 9640)\r\n        if call.Done():\r\n            \"\"\"State 4\"\"\"\r\n            Label('L0')\r\n            ChangeCamera(1000000)\r\n            call = t{id_s}_x13(val1=val1, z1=z1)\r\n            def WhilePaused():\r\n                ChangeCameraIf(GetDistanceToPlayer() > 2.5, -1)\r\n                RemoveMyAggroIf(IsAttackedBySomeone() and (DoesSelfHaveSpEffect(9626) and DoesSelfHaveSpEffect(9627)))\r\n                GiveSpEffectToPlayer(9640)\r\n                SetLookAtEntityForTalkIf(mode1 == 1, -1, 0)\r\n                SetLookAtEntityForTalkIf(mode2 == 1, 0, -1)\r\n            def ExitPause():\r\n                ChangeCamera(-1)\r\n            if call.Done():\r\n                continue\r\n            elif IsAttackedBySomeone():\r\n                pass\r\n        elif IsAttackedBySomeone() and not DoesSelfHaveSpEffect(9626) and not DoesSelfHaveSpEffect(9627):\r\n            pass\r\n        elif GetEventFlag(flag13):\r\n            Goto('L0')\r\n        elif GetEventFlag(flag11) and not GetEventFlag(flag12) and GetDistanceToPlayer() < val4:\r\n            \"\"\"State 5\"\"\"\r\n            call = t{id_s}_x15(val5=val5)\r\n            if call.Done():\r\n                continue\r\n            elif IsAttackedBySomeone():\r\n                pass\r\n        elif ((GetDistanceToPlayer() > val5 or GetTalkInterruptReason() == 6) and not CheckSpecificPersonTalkHasEnded(0)\r\n              and not DoesSelfHaveSpEffect(9625)):\r\n            \"\"\"State 6\"\"\"\r\n            assert t{id_s}_x26() and CheckSpecificPersonTalkHasEnded(0)\r\n            continue\r\n        elif GetEventFlag(9000):\r\n            \"\"\"State 1\"\"\"\r\n            assert not GetEventFlag(9000)\r\n            continue\r\n        \"\"\"State 3\"\"\"\r\n        def ExitPause():\r\n            RemoveMyAggro()\r\n        assert t{id_s}_x11(val2=val2, val3=val3)\r\n";
        }

        private string State_x7(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x7(val2=10, val3=12):\r\n    \"\"\"State 0,1\"\"\"\r\n    call = t{id_s}_x17(val2=val2, val3=val3)\r\n    assert IsPlayerDead()\r\n    \"\"\"State 2\"\"\"\r\n    t{id_s}_x3(val2=val2, val3=val3)\r\n    Quit()\r\n";
        }

        private string State_x8(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x8(flag6=4743, val2=10, val3=12):\r\n    \"\"\"State 0,8\"\"\"\r\n    assert t{id_s}_x36()\r\n    \"\"\"State 1\"\"\"\r\n    if GetEventFlag(flag6):\r\n        \"\"\"State 2\"\"\"\r\n        pass\r\n    else:\r\n        \"\"\"State 3\"\"\"\r\n        if GetDistanceToPlayer() < val2:\r\n            \"\"\"State 4,6\"\"\"\r\n            call = t{id_s}_x20()\r\n            if call.Done():\r\n                pass\r\n            elif GetDistanceToPlayer() > val3 or GetTalkInterruptReason() == 6:\r\n                \"\"\"State 7\"\"\"\r\n                assert t{id_s}_x1()\r\n        else:\r\n            \"\"\"State 5\"\"\"\r\n            pass\r\n    \"\"\"State 9\"\"\"\r\n    return 0\r\n";
        }

        private string State_x9(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x9(actionbutton1=6000, flag9=6000, flag10=6001, z2=1000000, z3=1000000, z4=1000000):\r\n    \"\"\"State 0,1\"\"\"\r\n    call = t{id_s}_x10(machine1=2000, val6=2000)\r\n    if call.Get() == 1:\r\n        \"\"\"State 2\"\"\"\r\n        assert (t{id_s}_x0(actionbutton1=actionbutton1, flag10=flag10, flag14=6000, flag15=6000, flag16=6000,\r\n                flag17=6000, flag9=flag9))\r\n    elif call.Done():\r\n        pass\r\n    \"\"\"State 3\"\"\"\r\n    return 0\r\n";
        }

        private string State_x10(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x10(machine1=_, val6=_):\r\n    \"\"\"State 0,1\"\"\"\r\n    if MachineExists(machine1):\r\n        \"\"\"State 2\"\"\"\r\n        assert GetCurrentStateElapsedFrames() > 1\r\n        \"\"\"State 4\"\"\"\r\n        def WhilePaused():\r\n            RunMachine(machine1)\r\n        assert GetMachineResult() == val6\r\n        \"\"\"State 5\"\"\"\r\n        return 0\r\n    else:\r\n        \"\"\"State 3,6\"\"\"\r\n        return 1\r\n";
        }

        private string State_x11(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x11(val2=10, val3=12):\r\n    \"\"\"State 0\"\"\"\r\n    assert GetCurrentStateElapsedFrames() > 1\r\n    \"\"\"State 5\"\"\"\r\n    assert t{id_s}_x1()\r\n    \"\"\"State 3\"\"\"\r\n    if GetDistanceToPlayer() < val2:\r\n        \"\"\"State 1\"\"\"\r\n        if IsPlayerAttacking():\r\n            \"\"\"State 6\"\"\"\r\n            call = t{id_s}_x12()\r\n            if call.Done():\r\n                pass\r\n            elif GetDistanceToPlayer() > val3 or GetTalkInterruptReason() == 6:\r\n                \"\"\"State 7\"\"\"\r\n                assert t{id_s}_x27()\r\n        else:\r\n            \"\"\"State 4\"\"\"\r\n            pass\r\n    else:\r\n        \"\"\"State 2\"\"\"\r\n        pass\r\n    \"\"\"State 8\"\"\"\r\n    return 0\r\n";
        }

        private string State_x12(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x12():\r\n    \"\"\"State 0,1\"\"\"\r\n    assert t{id_s}_x10(machine1=1101, val6=1101)\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x13(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x13(val1=5, z1=1):\r\n    \"\"\"State 0,2\"\"\"\r\n    assert t{id_s}_x23()\r\n    \"\"\"State 1\"\"\"\r\n    call = t{id_s}_x14()\r\n    if call.Done():\r\n        pass\r\n    elif (GetDistanceToPlayer() > val1 or GetTalkInterruptReason() == 6) and not DoesSelfHaveSpEffect(9625):\r\n        \"\"\"State 3\"\"\"\r\n        assert t{id_s}_x25()\r\n    \"\"\"State 4\"\"\"\r\n    return 0\r\n";
        }

        private string State_x14(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x14():\r\n    \"\"\"State 0,1\"\"\"\r\n    assert t{id_s}_x10(machine1=1000, val6=1000)\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x15(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x15(val5=12):\r\n    \"\"\"State 0,1\"\"\"\r\n    call = t{id_s}_x16()\r\n    if call.Done():\r\n        pass\r\n    elif GetDistanceToPlayer() > val5 or GetTalkInterruptReason() == 6:\r\n        \"\"\"State 2\"\"\"\r\n        assert t{id_s}_x26()\r\n    \"\"\"State 3\"\"\"\r\n    return 0\r\n";
        }

        private string State_x16(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x16():\r\n    \"\"\"State 0,1\"\"\"\r\n    assert t{id_s}_x10(machine1=1100, val6=1100)\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x17(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x17(val2=10, val3=12):\r\n    \"\"\"State 0,5\"\"\"\r\n    assert t{id_s}_x36()\r\n    \"\"\"State 2\"\"\"\r\n    assert not GetEventFlag(3000)\r\n    while True:\r\n        \"\"\"State 1\"\"\"\r\n        assert GetDistanceToPlayer() < val2\r\n        \"\"\"State 3\"\"\"\r\n        call = t{id_s}_x18()\r\n        if call.Done():\r\n            pass\r\n        elif GetDistanceToPlayer() > val3 or GetTalkInterruptReason() == 6:\r\n            \"\"\"State 4\"\"\"\r\n            assert t{id_s}_x28()\r\n";
        }

        private string State_x18(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x18():\r\n    \"\"\"State 0,2\"\"\"\r\n    call = t{id_s}_x10(machine1=1102, val6=1102)\r\n    if call.Get() == 1:\r\n        \"\"\"State 1\"\"\"\r\n        Quit()\r\n    elif call.Done():\r\n        \"\"\"State 3\"\"\"\r\n        return 0\r\n";
        }

        private string State_x19(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x19():\r\n    \"\"\"State 0,1\"\"\"\r\n    assert t{id_s}_x10(machine1=1001, val6=1001)\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x20(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x20():\r\n    \"\"\"State 0,1\"\"\"\r\n    assert t{id_s}_x10(machine1=1103, val6=1103)\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x21(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x21():\r\n    \"\"\"State 0\"\"\"\r\n    Quit()\r\n";
        }

        private string State_x22(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x22(flag6=4743, flag7=4741, flag8=4742, val1=5, val2=10, val3=12, val4=10, val5=12, actionbutton1=6000,\r\n                   flag9=6000, flag10=6001, flag11=6000, flag12=6000, flag13=6000, z1=1, z2=1000000, z3=1000000,\r\n                   z4=1000000, mode1=1, mode2=1):\r\n    \"\"\"State 0\"\"\"\r\n    while True:\r\n        \"\"\"State 1\"\"\"\r\n        RemoveMyAggro()\r\n        call = t{id_s}_x6(val1=val1, val2=val2, val3=val3, val4=val4, val5=val5, actionbutton1=actionbutton1,\r\n                             flag9=flag9, flag10=flag10, flag11=flag11, flag12=flag12, flag13=flag13, z1=z1, z2=z2,\r\n                             z3=z3, z4=z4, mode1=mode1, mode2=mode2)\r\n        if CheckSelfDeath() or GetEventFlag(flag6):\r\n            \"\"\"State 3\"\"\"\r\n            Label('L0')\r\n            call = t{id_s}_x8(flag6=flag6, val2=val2, val3=val3)\r\n            if not CheckSelfDeath() and not GetEventFlag(flag6):\r\n                continue\r\n            elif GetEventFlag(9000):\r\n                pass\r\n        elif GetEventFlag(flag7) or GetEventFlag(flag8):\r\n            \"\"\"State 2\"\"\"\r\n            call = t{id_s}_x7(val2=val2, val3=val3)\r\n            if CheckSelfDeath() or GetEventFlag(flag6):\r\n                Goto('L0')\r\n            elif not GetEventFlag(flag7) and not GetEventFlag(flag8):\r\n                continue\r\n            elif GetEventFlag(9000):\r\n                pass\r\n        elif GetEventFlag(9000) or IsPlayerDead():\r\n            pass\r\n        \"\"\"State 4\"\"\"\r\n        assert t{id_s}_x35() and not GetEventFlag(9000)\r\n";
        }

        private string State_x23(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x23():\r\n    \"\"\"State 0,1\"\"\"\r\n    assert t{id_s}_x24()\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x24(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x24():\r\n    \"\"\"State 0,1\"\"\"\r\n    assert t{id_s}_x10(machine1=1104, val6=1104)\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x25(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x25():\r\n    \"\"\"State 0,1\"\"\"\r\n    call = t{id_s}_x10(machine1=1201, val6=1201)\r\n    if call.Get() == 1:\r\n        \"\"\"State 2\"\"\"\r\n        assert t{id_s}_x4()\r\n    elif call.Done():\r\n        pass\r\n    \"\"\"State 3\"\"\"\r\n    return 0\r\n";
        }

        private string State_x26(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x26():\r\n    \"\"\"State 0,1\"\"\"\r\n    call = t{id_s}_x10(machine1=1300, val6=1300)\r\n    if call.Get() == 1:\r\n        \"\"\"State 2\"\"\"\r\n        assert t{id_s}_x4()\r\n    elif call.Done():\r\n        pass\r\n    \"\"\"State 3\"\"\"\r\n    return 0\r\n";
        }

        private string State_x27(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x27():\r\n    \"\"\"State 0,1\"\"\"\r\n    call = t{id_s}_x10(machine1=1301, val6=1301)\r\n    if call.Get() == 1:\r\n        \"\"\"State 2\"\"\"\r\n        assert t{id_s}_x4()\r\n    elif call.Done():\r\n        pass\r\n    \"\"\"State 3\"\"\"\r\n    return 0\r\n";
        }

        private string State_x28(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x28():\r\n    \"\"\"State 0,1\"\"\"\r\n    call = t{id_s}_x10(machine1=1302, val6=1302)\r\n    if call.Get() == 1:\r\n        \"\"\"State 2\"\"\"\r\n        assert t{id_s}_x4()\r\n    elif call.Done():\r\n        pass\r\n    \"\"\"State 3\"\"\"\r\n    return 0\r\n";
        }

        private string State_x29(uint id, NpcManager.TopicData greeting)
        {
            string id_s = id.ToString("D9");
            string s = $"def t{id_s}_x29(mode6=1):\r\n    \"\"\"State 0,4\"\"\"\r\n    assert t{id_s}_x2() and CheckSpecificPersonTalkHasEnded(0)\r\n    ShuffleRNGSeed(100)\r\n    SetRNGSeed()\r\n";

            // Build an if-else tree for each possible greeting and its conditions
            if (greeting.talks.Count > 1)
            {
                string ifop = "if";
                for (int i = 0; i < greeting.talks.Count(); i++)
                {
                    NpcManager.TopicData.TalkData talkData = greeting.talks[i];

                    string filters = $" {talkData.dialogInfo.GenerateCondition(scriptManager, npcContent)}";
                    string greetLine = "";
                    if (filters == " " || !(i < greeting.talks.Count() - 1)) { ifop = "else"; filters = ""; }

                    greetLine += $"    {ifop}{filters}:\r\n";
                    greetLine += $"        # talk: \"{Common.Utility.SanitizeTextForComment(talkData.dialogInfo.text)}\"\r\n";
                    greetLine += $"        TalkToPlayer({talkData.talkRow}, -1, -1, 0)\r\n        assert CheckSpecificPersonTalkHasEnded(0)\r\n";

                    foreach (DialogRecord dialog in talkData.dialogInfo.unlocks)
                    {
                        greetLine += $"        SetEventFlag({dialog.flag.id}, FlagState.On)\r\n";
                    }

                    if(talkData.dialogInfo.script != null)
                    {
                        greetLine += talkData.dialogInfo.script.GenerateEsdSnippet(scriptManager, npcContent, 8);
                    }

                    s += greetLine;

                    if (ifop == "if") { ifop = "elif"; }
                    if (ifop == "else") { break; }
                }
            }
            // Or if there is just a single possible greeting just stick there and call it done
            else
            {
                string greetLine = $"    TalkToPlayer({greeting.talks[0].talkRow}, -1, -1, 0)\r\n    assert CheckSpecificPersonTalkHasEnded(0)\r\n";
                foreach (DialogRecord dialog in greeting.talks[0].dialogInfo.unlocks)
                {
                    greetLine += $"    SetEventFlag({dialog.flag.id}, FlagState.On)\r\n";
                }
                s += greetLine;
            }
            s += "    \"\"\"State 3\"\"\"\r\n    if mode6 == 0:\r\n        pass\r\n    else:\r\n        \"\"\"State 2\"\"\"\r\n        ReportConversationEndToHavokBehavior()\r\n    \"\"\"State 5\"\"\"\r\n";
            // Also make sure to flag the TalkedToPC flag as it should be marked true once the player has finished the greeting with an npc for the first time
            s += $"    SetEventFlag({scriptManager.GetFlag(Script.Flag.Designation.TalkedToPc, npcContent.id).id}, FlagState.On)\r\n";
            s += "    return 0\r\n";
            return s;
        }

        private string State_x30(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x30(text3=_, mode5=1):\r\n    \"\"\"State 0,4\"\"\"\r\n    assert t{id_s}_x31() and CheckSpecificPersonTalkHasEnded(0)\r\n    \"\"\"State 1\"\"\"\r\n    TalkToPlayer(text3, -1, -1, 1)\r\n    \"\"\"State 3\"\"\"\r\n    if mode5 == 0:\r\n        pass\r\n    else:\r\n        \"\"\"State 2\"\"\"\r\n        ReportConversationEndToHavokBehavior()\r\n    \"\"\"State 5\"\"\"\r\n    return 0\r\n";
        }

        private string State_x31(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x31():\r\n    \"\"\"State 0,1\"\"\"\r\n    ClearTalkProgressData()\r\n    StopEventAnimWithoutForcingConversationEnd(0)\r\n    ReportConversationEndToHavokBehavior()\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x32(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x32():\r\n    \"\"\"State 0,1\"\"\"\r\n    assert t{id_s}_x10(machine1=1002, val6=1002)\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x33(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x33(text2=_, mode4=1):\r\n    assert t{id_s}_x2() and CheckSpecificPersonTalkHasEnded(0)\r\n    TalkToPlayer(text2, -1, -1, 0)\r\n    assert CheckSpecificPersonTalkHasEnded(0)\r\n    if mode4 == 0:\r\n        pass\r\n    else:\r\n        ReportConversationEndToHavokBehavior()\r\n    return 0\r\n";
        }

        private string State_x34(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x34(text1=_, flag3=_, mode3=1):\r\n    \"\"\"State 0,5\"\"\"\r\n    assert t{id_s}_x31() and CheckSpecificPersonTalkHasEnded(0)\r\n    \"\"\"State 2\"\"\"\r\n    SetEventFlag(flag3, FlagState.On)\r\n    \"\"\"State 1\"\"\"\r\n    TalkToPlayer(text1, -1, -1, 1)\r\n    \"\"\"State 4\"\"\"\r\n    if mode3 == 0:\r\n        pass\r\n    else:\r\n        \"\"\"State 3\"\"\"\r\n        ReportConversationEndToHavokBehavior()\r\n    \"\"\"State 6\"\"\"\r\n    return 0\r\n";
        }

        private string State_x35(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x35():\r\n    \"\"\"State 0,1\"\"\"\r\n    assert t{id_s}_x1()\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x36(uint id)
        {
            string id_s = id.ToString("D9");
            return $"def t{id_s}_x36():\r\n    \"\"\"State 0,1\"\"\"\r\n    if CheckSpecificPersonGenericDialogIsOpen(0):\r\n        \"\"\"State 2\"\"\"\r\n        ForceCloseGenericDialog()\r\n    else:\r\n        pass\r\n    \"\"\"State 3\"\"\"\r\n    if CheckSpecificPersonMenuIsOpen(-1, 0) and not CheckSpecificPersonGenericDialogIsOpen(0):\r\n        \"\"\"State 4\"\"\"\r\n        ForceCloseMenu()\r\n    else:\r\n        pass\r\n    \"\"\"State 5\"\"\"\r\n    return 0\r\n";
        }

        private string State_x37(uint id)
        {
            string id_s = id.ToString("D9");
            int shop1 = 100625;
            int shop2 = 100649;
            return $"def t{id_s}_x37(shop1={shop1}, shop2={shop2}):\r\n    \"\"\"State 0,1\"\"\"\r\n    assert t{id_s}_x43()\r\n    \"\"\"State 2\"\"\"\r\n    assert t{id_s}_x44(shop1=shop1, shop2=shop2)\r\n    \"\"\"State 3\"\"\"\r\n    return 0\r\n";
        }

        private string State_x38(uint id, int killTalk)
        {
            string id_s = id.ToString("D9");
            //int killTalk = 80181200;
            return $"def t{id_s}_x38():\r\n    \"\"\"State 0,1\"\"\"\r\n    # talk:80181200:\"Stay away, Us wanderers have had enough.\"\r\n    assert t{id_s}_x30(text3={killTalk}, mode5=1)\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x39(uint id, int hurt1Talk)
        {
            string id_s = id.ToString("D9");
            //int hurt1Talk = 80181000;
            //int hurt2Talk = 80181010;
            return $"def t{id_s}_x39(flag5=1043332705):\r\n    \"\"\"State 0,3\"\"\"\r\n    if not GetEventFlag(flag5):\r\n        \"\"\"State 1,4\"\"\"\r\n        # talk:80181000:\"Owgh!\"\r\n        assert t{id_s}_x34(text1={hurt1Talk}, flag3=flag5, mode3=1)\r\n    else:\r\n        \"\"\"State 2,5\"\"\"\r\n        # talk:80181010:\"What are you playing at! Stop this!\"\r\n        assert t{id_s}_x34(text1={hurt1Talk}, flag3=flag5, mode3=1)\r\n    \"\"\"State 6\"\"\"\r\n    return 0\r\n";
        }

        private string State_x40(uint id, int hostileTalk)
        {
            string id_s = id.ToString("D9");
            //int hostileTalk = 80181100;
            return $"def t{id_s}_x40(flag4=1043339205):\r\n    \"\"\"State 0,2\"\"\"\r\n    if not GetEventFlag(flag4):\r\n        \"\"\"State 3,5\"\"\"\r\n        # talk:80181100:\"That's the last straw, you bloody thief!\"\r\n        assert t{id_s}_x34(text1={hostileTalk}, flag3=flag4, mode3=1)\r\n    else:\r\n        \"\"\"State 4\"\"\"\r\n        pass\r\n    \"\"\"State 1\"\"\"\r\n    Quit()\r\n";
        }

        private string State_x41(uint id, int deathTalk)
        {
            string id_s = id.ToString("D9");
            //int deathTalk = 80181300;
            return $"def t{id_s}_x41():\r\n    \"\"\"State 0,1\"\"\"\r\n    # talk:80181300:\"How dare you trample us.\"\r\n    # talk:80181301:\"You filthy thief.\"\r\n    assert t{id_s}_x30(text3={deathTalk}, mode5=1)\r\n    \"\"\"State 2\"\"\"\r\n    return 0\r\n";
        }

        private string State_x42(uint id)
        {
            string id_s = id.ToString("D9");
            int unk0Flag = 1043332706;
            int unk1Flag = 1043332707;
            return $"def t{id_s}_x42(flag2={unk0Flag}, flag3={unk1Flag}):\r\n    \"\"\"State 0\"\"\"\r\n    while True:\r\n        \"\"\"State 1\"\"\"\r\n        # actionbutton:6000:\"Talk\"\r\n        call = t{id_s}_x0(actionbutton1=6000, flag10=6001, flag14=6000, flag15=6000, flag16=6000, flag17=6000,\r\n                             flag9=6000)\r\n        if call.Done():\r\n            break\r\n        elif GetEventFlag(flag2) and not GetEventFlag(flag3):\r\n            \"\"\"State 2\"\"\"\r\n            # talk:80181010:\"What are you playing at! Stop this!\"\r\n            assert t{id_s}_x34(text1=80181010, flag3=flag3, mode3=1)\r\n    \"\"\"State 3\"\"\"\r\n    return 0\r\n";
        }

        private string State_x43(uint id)
        {
            string id_s = id.ToString("D9");
            string s = $"def t{id_s}_x43():\r\n    \"\"\"State 0,1\"\"\"\r\n    # talk:80105100:\"Ah, back again are we?\"\r\n    # talk:80105101:\"Not everyone can tell how good my wares are. You've a discerning eye, you have.\"\r\n    assert t{id_s}_x29(mode6=1)\r\n    \"\"\"State 4\"\"\"\r\n    return 0\r\n";
            return s;
        }

        private string State_x44(uint id, bool hasShop, List<NpcManager.TopicData> topics)
        {
            string id_s = id.ToString("D9");
            int shop1 = 100625;
            int shop2 = 100649;
            string s = $"def t{id_s}_x44(shop1={shop1}, shop2={shop2}):\r\n    \"\"\"State 0\"\"\"\r\n    while True:\r\n        \"\"\"State 1\"\"\"\r\n        ClearPreviousMenuSelection()\r\n        ClearTalkActionState()\r\n        ClearTalkListData()\r\n        \"\"\"State 2\"\"\"\r\n";

            int listCount = 1; // starts at 1 idk
            if(hasShop)
            {
                s += $"        # action:20000010:\"Purchase\"\r\n        AddTalkListData({listCount++}, 20000010, -1)\r\n        # action:20000011:\"Sell\"\r\n        AddTalkListData({listCount++}, 20000011, -1)\r\n";
            }

            for (int i = 0; i < topics.Count(); i++)
            {
                NpcManager.TopicData topic = topics[i];
                List<string> filters = new();
                foreach(NpcManager.TopicData.TalkData talk in topic.talks)
                {
                    string filter = talk.dialogInfo.GenerateCondition(scriptManager, npcContent);
                    if(filter == "") { filters.Clear(); break; }
                    filters.Add(filter);
                }
                string combinedFilters = "";
                for(int j = 0;j<filters.Count();j++)
                {
                    string filter = filters[j];
                    combinedFilters += $"({filter})";
                    if(j<filters.Count()-1) { combinedFilters += " or "; }
                }
                if(combinedFilters != "") { combinedFilters = $" and ({combinedFilters})"; }

                s += $"        # action:{topic.topicText}:\"{topic.dialog.id}\"\r\n        if GetEventFlag({topic.dialog.flag.id}){combinedFilters}:\r\n            AddTalkListData({i+listCount}, {topic.topicText}, -1)\r\n        else:\r\n            pass\r\n";
            }

            s += $"        # action:20000009:\"Leave\"\r\n        AddTalkListData(99, 20000009, -1)\r\n        \"\"\"State 3\"\"\"\r\n        ShowShopMessage(TalkOptionsType.Regular)\r\n        \"\"\"State 4\"\"\"\r\n        assert not (CheckSpecificPersonMenuIsOpen(1, 0) and not CheckSpecificPersonGenericDialogIsOpen(0))\r\n        \"\"\"State 5\"\"\"\r\n";

            listCount = 1; // reset
            if (hasShop)
            {
                s += $"        if GetTalkListEntryResult() == {listCount++}:\r\n            \"\"\"State 6\"\"\"\r\n            OpenRegularShop(shop1, shop2)\r\n            \"\"\"State 7\"\"\"\r\n            assert not (CheckSpecificPersonMenuIsOpen(5, 0) and not CheckSpecificPersonGenericDialogIsOpen(0))\r\n        elif GetTalkListEntryResult() == {listCount++}:\r\n            \"\"\"State 9\"\"\"\r\n            OpenSellShop(-1, -1)\r\n            \"\"\"State 8\"\"\"\r\n            assert not (CheckSpecificPersonMenuIsOpen(6, 0) and not CheckSpecificPersonGenericDialogIsOpen(0))\r\n";
            }

            string ifop = hasShop?"elif":"if";
            for (int i = 0; i<topics.Count();i++)
            {
                NpcManager.TopicData topic = topics[i];

                foreach (NpcManager.TopicData.TalkData talk in topic.talks)
                {
                    string filters = talk.dialogInfo.GenerateCondition(scriptManager, npcContent);
                    if(filters != "") { filters = $" and {filters}"; }

                    s += $"        {ifop} GetTalkListEntryResult() == {i + listCount}{filters}:\r\n";
                    s += $"            # talk: \"{Common.Utility.SanitizeTextForComment(talk.dialogInfo.text)}\"\r\n";
                    s += $"            assert t{id_s}_x33(text2={talk.talkRow}, mode4=1)\r\n";

                    foreach (DialogRecord dialog in talk.dialogInfo.unlocks)
                    {
                        s += $"            SetEventFlag({dialog.flag.id}, FlagState.On)\r\n";
                    }

                    if (talk.dialogInfo.script != null)
                    {
                        s += talk.dialogInfo.script.GenerateEsdSnippet(scriptManager, npcContent, 12);
                    }


                    if (ifop == "if") { ifop = "elif"; }
                }
            }

            s += "        else:\r\n            \"\"\"State 10,11\"\"\"\r\n            return 0\r\n";
            return s;
        }
    }
}
