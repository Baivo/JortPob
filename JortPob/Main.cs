using HKLib.hk2018.hk;
using JortPob.Common;
using JortPob.Worker;
using PortJob;
using SoulsFormats;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Text.Json.Nodes;

namespace JortPob
{
    public class Main
    {
        public static void Convert()
        {
            /* Startup logging */
            Lort.Initialize();

            /* Loading stuff */
            ScriptManager scriptManager = new();                                                // Manages EMEVD scripts
            ESM esm = new ESM($"{Const.MORROWIND_PATH}\\Data Files\\Morrowind.json", scriptManager);      // Morrowind ESM parse and partial serialization
            Cache cache = Cache.Load(esm);                                                  // Load existing cache (FAST!) or generate a new one (SLOW!)
            Paramanager param = new();                                                        // Class for managing PARAM files
            TextManager text = new();                                                           // Manages FMG text files
            Layout layout = new(cache, esm, param, text, scriptManager);                          // Subdivides all content data from ESM into a more elden ring friendly format
            SoundManager sound = new();                                                         // Manages vcbanks
            NpcManager character = new(esm, sound, param, text, scriptManager);                 // Manages dialog esd

            /* Generate exterior msbs from layout */
            List<ResourcePool> msbs = new();

            Lort.Log($"Generating {layout.tiles.Count} exterior msbs...", Lort.Type.Main);
            Lort.NewTask("Generating MSB", layout.tiles.Count);
            foreach (BaseTile tile in layout.all)
            {
                if (tile.IsEmpty()) { continue; }   // Skip empty tiles.

                /* Generate msb from tile */
                MSBE msb = new();
                LightManager lightManager = new(tile.map, tile.coordinate, tile.block);
                Script script = scriptManager.GetScript(tile);
                ResourcePool pool = new(tile, msb, lightManager, script);
                msb.Compression = SoulsFormats.DCX.Type.DCX_KRAK;

                /* Variuos Indices */
                int nextC = 0, nextMPR = 0;

                /* Add terrain */
                foreach (Tuple<Vector3, TerrainInfo> tuple in tile.terrain)
                {
                    Vector3 position = tuple.Item1;
                    TerrainInfo terrainInfo = tuple.Item2;

                    /* Terrain and terrain collision */  // Render goes in superoverworld for long view distance. Collision goes in tile for optimization
                    // superoverowrld msb is  handled by its own class -> OverworldManager
                    if (tile.GetType() == typeof(Tile))
                    {
                        foreach (CollisionInfo collisionInfo in terrainInfo.collision)
                        {
                            string collisionIndex = $"{tile.coordinate.x.ToString("D2")}{tile.coordinate.y.ToString("D2")}{nextC++.ToString("D2")}";

                            MSBE.Part.Collision collision = MakePart.Collision();
                            collision.Name = $"h{collisionIndex}_0000";
                            collision.ModelName = $"h{collisionIndex}";
                            collision.Position = position + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;

                            msb.Parts.Collisions.Add(collision);
                            pool.collisionIndices.Add(new Tuple<string, CollisionInfo>(collisionIndex, collisionInfo));
                        }

                        /* Add water collision if terrain 'hasWater' */
                        /* Water is done on a cell by cell basis so I am simply tieing it to the terrain data here */
                        if (terrainInfo.hasWater)
                        {
                            LiquidInfo waterInfo = cache.GetWater();
                            CollisionInfo waterCollisionInfo = waterInfo.GetCollision(terrainInfo.coordinate);

                            /* Make collision for water splashing */
                            string collisionIndex = $"{tile.coordinate.x.ToString("D2")}{tile.coordinate.y.ToString("D2")}{nextC++.ToString("D2")}";
                            MSBE.Part.Collision collision = MakePart.WaterCollision();
                            collision.Name = $"h{collisionIndex}_0000";
                            collision.ModelName = $"h{collisionIndex}";
                            collision.Position = position + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;

                            msb.Parts.Collisions.Add(collision);
                            pool.collisionIndices.Add(new Tuple<string, CollisionInfo>(collisionIndex, waterCollisionInfo));
                        }
                        
                        /* Add collision for cutouts. */
                        if(terrainInfo.hasSwamp || terrainInfo.hasLava) // minor hack, no cell has both swamp and lava so we don't actually differentiate here
                        {
                            CutoutInfo cutoutInfo = cache.GetCutout(terrainInfo.coordinate);
                            if(cutoutInfo != null)
                            {
                                /* Make collision for swamp or lava splashy splashing, surface collision */
                                string collisionIndex = $"{tile.coordinate.x.ToString("D2")}{tile.coordinate.y.ToString("D2")}{nextC++.ToString("D2")}";
                                MSBE.Part.Collision collision = MakePart.WaterCollision(); // also works for lava and poison
                                collision.Name = $"h{collisionIndex}_0000";
                                collision.ModelName = $"h{collisionIndex}";
                                collision.Position = position + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                                msb.Parts.Collisions.Add(collision);
                                pool.collisionIndices.Add(new Tuple<string, CollisionInfo>(collisionIndex, cutoutInfo.collision));

                                /* Make collision for swamp or lava floor, caps the depth of pools so they arent super deep */
                                collision = MakePart.Collision(); // also works for lava and poison
                                collision.Name = $"h{collisionIndex}_0001";
                                collision.ModelName = $"h{collisionIndex}";
                                collision.Position = position + Const.TEST_OFFSET1 + Const.TEST_OFFSET2 + new Vector3(0f, terrainInfo.hasLava ? Const.LAVA_FLOOR_DEPTH : Const.SWAMP_FLOOR_DEPTH, 0f);
                                msb.Parts.Collisions.Add(collision);
                            }
                        }
                    }
                }

                /* Add assets */
                foreach (AssetContent content in tile.assets)
                {
                    /* Load overrides list for do not place @TODO: MAKE THIS A STATIC CLASS */
                    JsonNode json = JsonNode.Parse(File.ReadAllText(Utility.ResourcePath(@"overrides\do_not_place.json")));
                    bool CheckOverride(string name)
                    {
                        foreach (JsonNode node in json.AsArray())
                        {
                            if (node.ToString().ToLower() == name.ToLower()) { return true; }
                        }
                        return false;
                    }

                    if(CheckOverride(content.mesh)) { continue; } // SKIP!

                    /* Grab ModelInfo */
                    ModelInfo modelInfo = cache.GetModel(content.mesh, content.scale);

                    /* Make part */
                    MSBE.Part.Asset asset = MakePart.Asset(modelInfo);
                    asset.Position = content.relative + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                    asset.Rotation = content.rotation;
                    asset.Scale = new Vector3(modelInfo.UseScale()?(content.scale*0.01f):1f);

                    /* Asset tileload config */
                    if (tile.GetType() == typeof(HugeTile) || tile.GetType() == typeof(BigTile))
                    {
                        asset.TileLoad.MapID = new byte[] { (byte)0, (byte)content.load.y, (byte)content.load.x, (byte)tile.map };
                        asset.TileLoad.Unk04 = 13;
                        asset.TileLoad.CullingHeightBehavior = -1;
                    }

                    msb.Parts.Assets.Add(asset);
                }

                /* Add doors */
                foreach (DoorContent content in tile.doors)
                {
                    /* Grab ModelInfo */
                    ModelInfo modelInfo = cache.GetModel(content.mesh, content.scale);

                    /* Make part */
                    MSBE.Part.Asset asset = MakePart.Asset(modelInfo);
                    asset.Position = content.relative + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                    asset.Rotation = content.rotation;
                    asset.Scale = new Vector3(modelInfo.UseScale() ? (content.scale * 0.01f) : 1f);
                    asset.EntityID = content.entity;

                    if (content.warp != null)
                    {
                        script.RegisterLoadDoor(content);
                    }

                    msb.Parts.Assets.Add(asset);
                }

                /* Add warp destinations for load doors */
                foreach (Layout.WarpDestination warp in tile.warps)
                {
                    MSBE.Part.Player player = MakePart.Player();
                    player.Position = warp.position + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                    player.Rotation = warp.rotation;
                    player.EntityID = warp.id;
                    msb.Parts.Players.Add(player);
                }

                /* Add emitters */
                foreach (EmitterContent content in tile.emitters)
                {
                    /* Grab ModelInfo */
                    EmitterInfo emitterInfo = cache.GetEmitter(content.id);

                    /* Make part */
                    MSBE.Part.Asset asset = MakePart.Asset(emitterInfo);
                    asset.Position = content.relative + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                    asset.Rotation = content.rotation;
                    asset.Scale = new Vector3(content.scale * 0.01f);

                    msb.Parts.Assets.Add(asset);
                }

                /* Add lights */
                foreach (LightContent light in tile.lights)
                {
                    lightManager.CreateLight(light);
                }

                /* TEST NPCs */  // make some c0000 npcs where humanoid npcs would spawn as a test
                foreach (NpcContent npc in tile.npcs)
                {
                    MSBE.Part.Enemy enemy = MakePart.Npc();
                    enemy.Position = npc.relative + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                    enemy.Rotation = npc.rotation;

                    enemy.TalkID = character.GetESD(tile.IdList(), npc);       // creates and returns a character esd
                    enemy.NPCParamID = character.GetParam(npc); // creates and returns an npcparam

                    msb.Parts.Enemies.Add(enemy);
                }

                /* TEST Creatures */  // make some goats where enemies would spawn just as a test
                foreach (CreatureContent creature in tile.creatures)
                {
                    MSBE.Part.Enemy enemy = MakePart.Creature();
                    enemy.Position = creature.relative + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                    enemy.Rotation = creature.rotation;

                    msb.Parts.Enemies.Add(enemy);
                }

                /* Handle area names */
                if (tile.GetType() == typeof(Tile))
                {
                    foreach (Cell cell in tile.cells)
                    {
                        if (cell.name != null)
                        {
                            float x = (tile.coordinate.x * Const.TILE_SIZE);
                            float y = (tile.coordinate.y * Const.TILE_SIZE);
                            Vector3 relative = (cell.center + Const.LAYOUT_COORDINATE_OFFSET) - new Vector3(x, 0, y);

                            int paramId = int.Parse($"61{tile.coordinate.x:D2}{tile.coordinate.y:D2}{nextMPR:D2}");


                            MSBE.Region.MapPoint mpr = new();
                            mpr.Name = $"{cell.name} placename";
                            mpr.Shape = new MSB.Shape.Box(Const.CELL_SIZE, Const.CELL_SIZE, Const.CELL_SIZE * 8);
                            mpr.Position = relative;
                            mpr.Rotation = Vector3.Zero;
                            mpr.RegionID = nextMPR++;
                            mpr.MapStudioLayer = 4294967295;
                            mpr.WorldMapPointParamID = param.GenerateWorldMapPoint(text, tile, cell, relative, paramId);

                            mpr.MapID = -1;
                            mpr.UnkE08 = 255;
                            mpr.UnkS04 = 0;
                            mpr.UnkS0C = -1;
                            mpr.UnkT04 = -1;
                            mpr.UnkT08 = -1;
                            mpr.UnkT0C = -1;
                            mpr.UnkT10 = -1;
                            mpr.UnkT14 = -1;
                            mpr.UnkT18 = -1;

                            msb.Regions.MapPoints.Add(mpr);
                        }
                    }
                }

                /* Auto resource */
                AutoResource.Generate(tile.map, tile.coordinate.x, tile.coordinate.y, tile.block, msb);

                /* Done */
                msbs.Add(pool);
                Lort.TaskIterate(); // Progress bar update
            }

            /* Generate interior msbs from interiorgroups */
            Lort.Log($"Generating {layout.interiors.Count} interior msbs...", Lort.Type.Main);
            Lort.NewTask("Generating MSB", layout.interiors.Count);
            foreach (InteriorGroup group in layout.interiors)
            {
                if (Const.DEBUG_SKIP_INTERIOR) { break; }

                if (group.IsEmpty()) { continue; }   // Skip empty groups.

                /* Misc Indices */
                int nextC = 0, nextMPR = 0;

                /* Generate msb from group */
                MSBE msb = new();
                LightManager lightManager = new(group.map, group.area, group.unk, group.block);
                Script script = scriptManager.GetScript(group);
                ResourcePool pool = new(group, msb, lightManager, script);
                msb.Compression = SoulsFormats.DCX.Type.DCX_KRAK;

                /* Handle chunks */
                for (int i=0;i<group.chunks.Count();i++)
                {
                    InteriorGroup.Chunk chunk = group.chunks[i];

                    /* Interior MSB drawgroup */
                    uint chunkDrawGroup = (uint)0 | ((uint)1 << i);

                    /* Interior MSB chunk collision root */
                    string collisionIndex = $"{group.area.ToString("D2")}{group.unk.ToString("D2")}{nextC++.ToString("D2")}";
                    MSBE.Part.Collision rootCollision = MakePart.Collision();
                    rootCollision.Name = $"h{collisionIndex}_0000";
                    rootCollision.ModelName = $"h{collisionIndex}";
                    rootCollision.Position = chunk.root + Const.TEST_OFFSET1 + Const.TEST_OFFSET2 - new Vector3(0f, chunk.bounds.Z, 0f);
                    rootCollision.Unk1.DisplayGroups[0] = 0;
                    rootCollision.Unk1.DisplayGroups[1] = chunkDrawGroup;
                    rootCollision.Unk1.CollisionMask[0] = 0;
                    rootCollision.Unk1.CollisionMask[1] = chunkDrawGroup;
                    msb.Parts.Collisions.Add(rootCollision);
                    pool.collisionIndices.Add(new Tuple<string, CollisionInfo>(collisionIndex, cache.defaultCollision));

                    /* Interior MSB shadow box */
                    ModelInfo shadowBoxModelInfo = cache.GetModel("interiorshadowbox");
                    MSBE.Part.Asset shadowBoxAsset = MakePart.Asset(shadowBoxModelInfo);
                    shadowBoxAsset.Position = chunk.root + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                    shadowBoxAsset.Rotation = Vector3.Zero;
                    shadowBoxAsset.Scale = chunk.bounds;
                    shadowBoxAsset.Unk1.DisplayGroups[0] = 0;
                    shadowBoxAsset.UnkPartNames[1] = rootCollision.Name;
                    shadowBoxAsset.UnkPartNames[3] = rootCollision.Name;
                    shadowBoxAsset.UnkPartNames[5] = rootCollision.Name;
                    msb.Parts.Assets.Add(shadowBoxAsset);

                    /* Add assets */
                    foreach (AssetContent content in chunk.assets)
                    {
                        /* Load overrides list for do not place @TODO: MAKE THIS A STATIC CLASS */
                        JsonNode json = JsonNode.Parse(File.ReadAllText(Utility.ResourcePath(@"overrides\do_not_place.json")));
                        bool CheckOverride(string name)
                        {
                            foreach (JsonNode node in json.AsArray())
                            {
                                if (node.ToString().ToLower() == name.ToLower()) { return true; }
                            }
                            return false;
                        }

                        if (CheckOverride(content.mesh)) { continue; } // SKIP!

                        /* Grab ModelInfo */
                        ModelInfo modelInfo = cache.GetModel(content.mesh, content.scale);

                        /* Make part */
                        MSBE.Part.Asset asset = MakePart.Asset(modelInfo);
                        asset.Position = content.relative + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                        asset.Rotation = content.rotation;
                        asset.Scale = new Vector3(modelInfo.UseScale() ? (content.scale * 0.01f) : 1f);

                        asset.Unk1.DisplayGroups[0] = 0;
                        asset.UnkPartNames[1] = rootCollision.Name;
                        asset.UnkPartNames[3] = rootCollision.Name;
                        asset.UnkPartNames[5] = rootCollision.Name;

                        msb.Parts.Assets.Add(asset);
                    }

                    /* Add doors */
                    foreach (DoorContent content in chunk.doors)
                    {
                        /* Grab ModelInfo */
                        ModelInfo modelInfo = cache.GetModel(content.mesh, content.scale);

                        /* Make part */
                        MSBE.Part.Asset asset = MakePart.Asset(modelInfo);
                        asset.Position = content.relative + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                        asset.Rotation = content.rotation;
                        asset.Scale = new Vector3(modelInfo.UseScale() ? (content.scale * 0.01f) : 1f);
                        asset.EntityID = content.entity;

                        asset.Unk1.DisplayGroups[0] = 0;
                        asset.UnkPartNames[1] = rootCollision.Name;
                        asset.UnkPartNames[3] = rootCollision.Name;
                        asset.UnkPartNames[5] = rootCollision.Name;

                        if (content.warp != null)
                        {
                            script.RegisterLoadDoor(content);
                        }

                        msb.Parts.Assets.Add(asset);
                    }

                    /* Add warp destinations for load doors */
                    foreach(Layout.WarpDestination warp in chunk.warps)
                    {
                        MSBE.Part.Player player = MakePart.Player();
                        player.Position = warp.position + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                        player.Rotation = warp.rotation;
                        player.EntityID = warp.id;
                        msb.Parts.Players.Add(player);
                    }

                    /* Add emitters */
                    foreach (EmitterContent content in chunk.emitters)
                    {
                        /* Grab ModelInfo */
                        EmitterInfo emitterInfo = cache.GetEmitter(content.id);

                        /* Make part */
                        MSBE.Part.Asset asset = MakePart.Asset(emitterInfo);
                        asset.Position = content.relative + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                        asset.Rotation = content.rotation;
                        asset.Scale = new Vector3(content.scale * 0.01f);

                        asset.Unk1.DisplayGroups[0] = 0;
                        asset.UnkPartNames[1] = rootCollision.Name;
                        asset.UnkPartNames[3] = rootCollision.Name;
                        asset.UnkPartNames[5] = rootCollision.Name;

                        msb.Parts.Assets.Add(asset);
                    }

                    /* Add lights */
                    foreach (LightContent light in chunk.lights)
                    {
                        lightManager.CreateLight(light);
                    }

                    /* TEST NPCs */  // make some c0000 npcs where humanoid npcs would spawn as a test
                    foreach (NpcContent npc in chunk.npcs)
                    {
                        MSBE.Part.Enemy enemy = MakePart.Npc();
                        enemy.Position = npc.relative + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                        enemy.Rotation = npc.rotation;

                        enemy.TalkID = character.GetESD(group.IdList(), npc);       // creates and returns a character esd
                        enemy.NPCParamID = character.GetParam(npc); // creates and returns an npcparam

                        enemy.Unk1.DisplayGroups[0] = 0;
                        enemy.CollisionPartName = rootCollision.Name;

                        msb.Parts.Enemies.Add(enemy);
                    }

                    /* TEST Creatures */  // make some goats where enemies would spawn just as a test
                    foreach (CreatureContent creature in chunk.creatures)
                    {
                        MSBE.Part.Enemy enemy = MakePart.Creature();
                        enemy.Position = creature.relative + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                        enemy.Rotation = creature.rotation;

                        enemy.Unk1.DisplayGroups[0] = 0;
                        enemy.CollisionPartName = rootCollision.Name;

                        msb.Parts.Enemies.Add(enemy);
                    }

                    /* Handle area name */
                    int paramId = int.Parse($"60{group.map:D2}{group.area:D2}{nextMPR:D2}");


                    MSBE.Region.MapPoint mpr = new();
                    mpr.Name = $"{chunk.cell.name} placename";
                    mpr.Shape = new MSB.Shape.Box(chunk.bounds.X, chunk.bounds.Z, chunk.bounds.Y);
                    mpr.Position = chunk.root + Const.TEST_OFFSET1 + Const.TEST_OFFSET2 - new Vector3(0f, chunk.bounds.Y / 2f, 0f);
                    mpr.Rotation = Vector3.Zero;
                    mpr.RegionID = nextMPR++;
                    mpr.MapStudioLayer = 4294967295;
                    mpr.WorldMapPointParamID = param.GenerateWorldMapPoint(text, group, chunk.cell, chunk.root, paramId);

                    mpr.MapID = -1;
                    mpr.UnkE08 = 255;
                    mpr.UnkS04 = 0;
                    mpr.UnkS0C = -1;
                    mpr.UnkT04 = -1;
                    mpr.UnkT08 = -1;
                    mpr.UnkT0C = -1;
                    mpr.UnkT10 = -1;
                    mpr.UnkT14 = -1;
                    mpr.UnkT18 = -1;

                    msb.Regions.MapPoints.Add(mpr);
                }

                /* EnvMap & REM for interior */ // @TODO: make this per chunk so we can setupd different rems for different interiors
                {
                    /* Create envmap texture file */
                    int envId = 200;
                    int size = 4096, crossfade = 8;
                    EnvManager.CreateEnvMaps(group, envId);

                    /* Create an envbox */
                    MSBE.Region.EnvironmentMapEffectBox envBox = MakePart.EnvBox();
                    envBox.Name = $"Env_Box{envId.ToString("D3")}";
                    envBox.Shape = new MSB.Shape.Box(size + crossfade, size + crossfade, size + crossfade);
                    envBox.Position = new Vector3(0f, size * -0.5f, 0f) + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                    envBox.TransitionDist = crossfade / 2f;
                    msb.Regions.EnvironmentMapEffectBoxes.Add(envBox);

                    MSBE.Region.EnvironmentMapPoint envPoint = MakePart.EnvPoint();
                    envPoint.Name = $"Env_Point{envId.ToString("D3")}";
                    envPoint.Position = new Vector3(0f, size * -0.5f, 0f) + Const.TEST_OFFSET1 + Const.TEST_OFFSET2;
                    envPoint.UnkMapID = new byte[] { (byte)group.map, (byte)group.area, (byte)group.unk, (byte)group.block };
                    msb.Regions.EnvironmentMapPoints.Add(envPoint);
                }



                /* Auto resource */
                AutoResource.Generate(group.map, group.area, group.unk, group.block, msb);

                /* Done */
                msbs.Add(pool);
                Lort.TaskIterate(); // Progress bar update
            }

            {
                /* DEBUG - Add a warp from church of elleh to Seyda Neen */
                /* @TODO: DELETE THIS WHEN IT IS NO LONGER NEEDED! */
                MSBE debugMSB = MSBE.Read(Utility.ResourcePath(@"test\m60_42_36_00.msb.dcx"));
                MSBE.Part.Asset debugThingToDupe = null;
                uint debugEntityIdNext = 1042360750;
                foreach (MSBE.Part.Asset asset in debugMSB.Parts.Assets)
                {
                    if (asset.ModelName == "AEG099_309") { debugThingToDupe = asset; break; }
                }

                Script debugWarpScript = new(scriptManager.common, 60, 42, 36, 0);
                debugWarpScript.init.Instructions.Add(debugWarpScript.AUTO.ParseAdd($"RegisterBonfire(1042360000, 1042361950, 0, 0, 0, 5);"));
                debugWarpScript.init.Instructions.Add(debugWarpScript.AUTO.ParseAdd($"RegisterBonfire(1042360001, 1042361951, 0, 0, 0, 5);"));
                List<String> debugWarpCellList = new() { "Seyda Neen", "Balmora", "Tel Mora", "Pelagiad", "Caldera", "Khuul", "Gnisis", "Ald Ruhn" };
                int actionParamId = 1555, debugCounty = 0;
                for (int i=0;i<debugWarpCellList.Count();i++)
                {
                    string areaName = debugWarpCellList[i];
                    Tile area = layout.GetTile(areaName);
                    if (area != null && area.warps.Count() > 0)
                    {
                        MSBE.Part.Asset debugAsset = (MSBE.Part.Asset)(debugThingToDupe.DeepCopy());
                        debugAsset.Position = debugAsset.Position + new Vector3(2f * i, 0.5f, -1.1f * i);
                        debugAsset.EntityID = debugEntityIdNext++;
                        debugAsset.Name = $"AEG099_309_{9001 + debugCounty}";
                        debugAsset.UnkPartNames[4] = $"AEG099_309_{9001 + debugCounty}";
                        debugAsset.UnkPartNames[5] = $"AEG099_309_{9001 + debugCounty}";
                        debugAsset.UnkT54PartName = $"AEG099_309_{9001 + debugCounty}";
                        debugAsset.InstanceID++;
                        debugMSB.Parts.Assets.Add(debugAsset);

                        Script.Flag debugEventFlag = debugWarpScript.CreateFlag(Script.Flag.Category.Event, Script.Flag.Type.Bit, Script.Flag.Designation.Event, $"m{debugWarpScript.map}_{debugWarpScript.x}_{debugWarpScript.y}_{debugWarpScript.block}::DebugWarp");
                        EMEVD.Event debugWarpEvent = new(debugEventFlag.id);

                        param.GenerateActionButtonParam(text, actionParamId, $"Debug Warp: {areaName}");
                        debugWarpEvent.Instructions.Add(debugWarpScript.AUTO.ParseAdd($"IfActionButtonInArea(MAIN, {actionParamId}, {debugAsset.EntityID});"));
                        debugWarpEvent.Instructions.Add(debugWarpScript.AUTO.ParseAdd($"WarpPlayer({area.map}, {area.coordinate.x}, {area.coordinate.y}, 0, {area.warps[0].id}, 0)"));

                        debugWarpScript.init.Instructions.Add(debugWarpScript.AUTO.ParseAdd($"InitializeEvent(0, {debugEventFlag.id})"));

                        debugWarpScript.emevd.Events.Add(debugWarpEvent);
                        actionParamId++;
                        debugCounty++;
                    }
                }
                debugWarpScript.Write();
                debugMSB.Write($"{Const.OUTPUT_PATH}\\map\\mapstudio\\m60_42_36_00.msb.dcx");
            }

            /* Write sound BNKs */
            sound.Write($"{Const.OUTPUT_PATH}sd\\enus\\");

            /* Write ESD bnds */
            character.Write();

            /* Generate some params and write to file */
            Lort.Log($"Creating PARAMs...", Lort.Type.Main);
            param.GeneratePartDrawParams();
            param.GenerateAssetRows(cache.assets);
            param.GenerateAssetRows(cache.emitters);
            param.GenerateAssetRows(cache.liquids);
            param.GenerateMapInfoParam(layout);
            param.GenerateActionButtonParam(text, 1500, "Enter");
            param.GenerateActionButtonParam(text, 1501, "Exit");
            param.SetAllMapLocation(text);
            param.KillMapHeightParams();    // murder kill
            param.Write();

            /* Write FMGs */
            Lort.Log($"Binding FMGs...", Lort.Type.Main);
            text.Write($"{Const.OUTPUT_PATH}msg\\engus\\");

            /* Write FXR files */
            Lort.Log($"Binding FXRs...", Lort.Type.Main);
            FxrManager.Write(layout);

            /* Bind and write all materials and textures */
            Bind.BindMaterials($"{Const.OUTPUT_PATH}material\\allmaterial.matbinbnd.dcx");
            Bind.BindTPF(cache, layout.ListCommon());

            /* Bind all assets */    // Multithreaded because slow
            Lort.Log($"Binding {cache.assets.Count} assets...", Lort.Type.Main);
            Lort.NewTask("Binding Assets", cache.assets.Count);
            Bind.BindAssets(cache);
            Bind.BindEmitters(cache);
            foreach(LiquidInfo water in cache.liquids)  // bind up them waters toooooo
            {
                Bind.BindAsset(water, $"{Const.OUTPUT_PATH}asset\\aeg\\{water.AssetPath()}.geombnd.dcx");
            }

            /* Generate overworld */
            ResourcePool overworld = OverworldManager.Generate(cache, esm, layout, param);
            msbs.Insert(0, overworld); // this one takes the longest so we put it first so that the thread working on it has plenty of time to finish

            /* Write emevd scripts */
            scriptManager.Write();

            /* Write msbs */
            esm = null;  // free some memory here
            param = null;
            GC.Collect();
            MsbWorker.Go(msbs);

            /* Donezo */
            Lort.Log("Done!", Lort.Type.Main);
            Lort.NewTask("Done!", 1);
            Lort.TaskIterate();
        }
    }

    public class ResourcePool
    {
        public int[] id;
        public List<Tuple<int, string>> mapIndices;
        public MSBE msb;
        public LightManager lights;
        public Script script;
        public List<Tuple<string, CollisionInfo>> collisionIndices;

        /* Exterior cells */
        public ResourcePool(BaseTile tile, MSBE msb, LightManager lights, Script script = null)
        {
            id = new int[]
            {
                    tile.map, tile.coordinate.x, tile.coordinate.y, tile.block
            };
            mapIndices = new();
            collisionIndices = new();
            this.msb = msb;
            this.lights = lights;
            this.script = script;
        }

        /* Interior cells */
        public ResourcePool(InteriorGroup group, MSBE msb, LightManager lights, Script script = null)
        {
            id = new int[]
            {
                    group.map, group.area, group.unk, group.block
            };
            mapIndices = new();
            this.msb = msb;
            this.lights = lights;
            this.script = script;
            collisionIndices = new();
        }

        /* Super overworld */
        public ResourcePool(MSBE msb, LightManager lights)
        {
            id = new int[]
            {
                    60, 00, 00, 99
            };
            mapIndices = new();
            this.msb = msb;
            this.lights = lights;
            script = null;
            collisionIndices = new();
        }

        public void Add(TerrainInfo terrain)
        {
            mapIndices.Add(new Tuple<int, string>(terrain.id, terrain.path));
        }

        public void Add(string index, CollisionInfo collision)
        {
            collisionIndices.Add(new Tuple<string, CollisionInfo>(index, collision));
        }
    }
}
