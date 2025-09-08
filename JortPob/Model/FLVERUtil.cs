using JortPob.Common;
using SoulsFormats;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;

namespace JortPob.Model
{
    public class FLVERUtil
    {
        /* opens a flver, scales it, writes it */
        public static void Scale(string flverPath, string outPath, float scale)
        {
            /* Load flver */
            FLVER2 flver = FLVER2.Read(flverPath);

            /* Scale vertices... */
            foreach(FLVER2.Mesh mesh in flver.Meshes)
            {
                foreach(FLVER.Vertex vertex in mesh.Vertices)
                {
                    vertex.Position *= scale;
                }
            }

            /* Resolve bounding boxes after scaling */
            BoundingBoxSolver.FLVER(flver);

            /* Write to file */
            flver.Write(outPath);
        }

        /* takes a flver already loaded in memory and scans through it, removing anything that is not needed. */
        /* Examples: unused materials, duplicate vertices */
        public static FLVER2 Optimize(FLVER2 flver)
        {
            /* Delete unused materials */
            HashSet<int> usedMaterials = new();
            foreach (FLVER2.Mesh mesh in flver.Meshes)
            {
                usedMaterials.Add(mesh.MaterialIndex);
            }
            for (int i = flver.Materials.Count - 1; i >= 0; i--)
            {
                if (!usedMaterials.Contains(i))
                {
                    flver.Materials.RemoveAt(i);
                    foreach (FLVER2.Material mat in flver.Materials)
                    {
                        if (mat.Index > i) { mat.Index--; }
                    }
                    foreach (FLVER2.Mesh mesh in flver.Meshes)
                    {
                        if (mesh.MaterialIndex > i) { mesh.MaterialIndex--; }
                    }
                }
            }

            /* Delete unused bufferlayouts */
            HashSet<int> usedLayouts = new();
            foreach (FLVER2.Mesh mesh in flver.Meshes)
            {
                foreach (FLVER2.VertexBuffer buffer in mesh.VertexBuffers)
                {
                    usedLayouts.Add(buffer.LayoutIndex);
                }
            }

            for (int i = flver.BufferLayouts.Count - 1; i >= 0; i--)
            {
                if (!usedLayouts.Contains(i))
                {
                    flver.BufferLayouts.RemoveAt(i);
                    foreach (FLVER2.Mesh mesh in flver.Meshes)
                    {
                        foreach (FLVER2.VertexBuffer vb in mesh.VertexBuffers)
                        {
                            if (vb.LayoutIndex > i) { vb.LayoutIndex--; }
                        }
                    }
                }
            }


            /* Delete unused gxlists */
            HashSet<int> usedGXLists = new();
            foreach (FLVER2.Material mat in flver.Materials)
            {
                usedGXLists.Add(mat.GXIndex);
            }
            for (int i = flver.GXLists.Count - 1; i >= 0; i--)
            {
                if (!usedGXLists.Contains(i))
                {
                    flver.GXLists.RemoveAt(i);
                    foreach (FLVER2.Material mat in flver.Materials)
                    {
                        if (mat.GXIndex > i) { mat.GXIndex--; }
                    }
                }
            }

            /* Re-index duplicate vertices */
            foreach(FLVER2.Mesh mesh in flver.Meshes)
            {
                List<FLVER.Vertex> verts = mesh.Vertices; // original vertices
                mesh.Vertices = new();

                Dictionary<VertexKey, int> vertexLookup = new();

                int GetIndex(FLVER.Vertex v)
                {
                    VertexKey key = new VertexKey(v);

                    if (vertexLookup.TryGetValue(key, out int existingIndex))
                    {
                        return existingIndex;
                    }

                    int newIndex = mesh.Vertices.Count;
                    mesh.Vertices.Add(v);
                    vertexLookup[key] = newIndex;
                    return newIndex;
                }

                foreach (FLVER2.FaceSet faceSet in mesh.FaceSets)
                {
                    List<int> inds = faceSet.Indices; // original indices
                    faceSet.Indices = new();

                    for(int i = 0; i < inds.Count; i++)
                    {
                        FLVER.Vertex v = verts[inds[i]]; // get original vert from this index
                        int index = GetIndex(v);
                        faceSet.Indices.Add(index);
                    }
                }

            }

            return flver;
        }

        private struct VertexKey : IEquatable<VertexKey>
        {
            private readonly Vector3 position;
            private readonly Vector3 normal;
            private readonly List<Vector3> uvs;
            private readonly int hashCode;

            public VertexKey(FLVER.Vertex vertex)
            {
                position = vertex.Position;
                normal = vertex.Normal;
                foreach(Vector3 uv in  vertex.UVs)
                {
                    uvs.Add(uv);
                }

                string uvCombinedHash = "";
                foreach (Vector3 uv in uvs)
                {
                    uvCombinedHash += $"[{uv.X},{uv.Y}]";  // prolly not ideal but i dont want a 8 way switch on uv.count() or smth like that
                }

                hashCode = HashCode.Combine(position, normal, uvCombinedHash);
            }

            public bool Equals(VertexKey other)
            {
                if (uvs.Count() != other.uvs.Count()) { return false; }

                for (int i = 0; i < uvs.Count(); i++)
                {
                    if (!uvs[i].TolerantEquals(other.uvs[i], 0.0001f)) { return false; }
                }

                return position.TolerantEquals(other.position, 0.0001f) &&
                       normal.TolerantEquals(other.normal, 0.0001f);
            }

            public override bool Equals(object obj)
            {
                return obj is VertexKey other && Equals(other);
            }

            public override int GetHashCode()
            {
                return hashCode;
            }
        }
    }
}
