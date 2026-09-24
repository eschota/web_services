using System.Collections.Generic;
using UnityEngine;

namespace AutoRig.Cloth.Samples
{
    /// <summary>
    /// Renders neighbouring bone chains as a double-sided cloth strip (a skirt or a cape). Runs after
    /// the cloth manager, so it shows the simulated bones of the current frame.
    /// </summary>
    [DefaultExecutionOrder(AutoRigClothManager.ExecutionOrder + 100)]
    [RequireComponent(typeof(MeshFilter), typeof(MeshRenderer))]
    public sealed class ChainStripMesh : MonoBehaviour
    {
        private readonly List<Vector3> _vertices = new List<Vector3>();
        private Transform[][] _chains;
        private bool _loop;
        private int _rows;
        private Mesh _mesh;

        /// <summary>Builds the strip over <paramref name="chains"/>, closing it when <paramref name="loop"/> is set.</summary>
        public void Initialize(Transform[][] chains, bool loop, Material material)
        {
            _chains = chains;
            _loop = loop;
            _rows = int.MaxValue;
            for (int c = 0; c < chains.Length; c++)
            {
                _rows = Mathf.Min(_rows, chains[c].Length);
            }

            int columns = chains.Length + (loop ? 1 : 0);
            int perSide = columns * _rows;
            var triangles = new List<int>();
            for (int c = 0; c < columns - 1; c++)
            {
                for (int r = 0; r < _rows - 1; r++)
                {
                    int a = c * _rows + r;
                    int b = (c + 1) * _rows + r;
                    triangles.AddRange(new[] { a, a + 1, b, b, a + 1, b + 1 });
                    triangles.AddRange(new[] { perSide + a, perSide + b, perSide + a + 1, perSide + b, perSide + b + 1, perSide + a + 1 });
                }
            }

            _mesh = new Mesh { name = "Cloth Strip" };
            _mesh.MarkDynamic();
            UpdateVertices();
            _mesh.SetTriangles(triangles, 0);
            _mesh.RecalculateNormals();
            GetComponent<MeshFilter>().sharedMesh = _mesh;
            GetComponent<MeshRenderer>().sharedMaterial = material;
        }

        private void LateUpdate()
        {
            if (_mesh == null)
            {
                return;
            }

            UpdateVertices();
            _mesh.RecalculateNormals();
            _mesh.RecalculateBounds();
        }

        private void OnDestroy()
        {
            if (_mesh != null)
            {
                Destroy(_mesh);
            }
        }

        private void UpdateVertices()
        {
            _vertices.Clear();
            int columns = _chains.Length + (_loop ? 1 : 0);
            for (int side = 0; side < 2; side++)
            {
                for (int c = 0; c < columns; c++)
                {
                    Transform[] chain = _chains[c % _chains.Length];
                    for (int r = 0; r < _rows; r++)
                    {
                        _vertices.Add(transform.InverseTransformPoint(chain[r].position));
                    }
                }
            }

            _mesh.SetVertices(_vertices);
        }
    }
}
