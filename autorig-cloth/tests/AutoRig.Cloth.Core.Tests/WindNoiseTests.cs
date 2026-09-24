using System;
using AutoRig.Cloth.Core;
using Xunit;

namespace AutoRig.Cloth.Core.Tests
{
    public class WindNoiseTests
    {
        [Fact]
        public void Sample_IsDeterministicAndBounded()
        {
            var random = new Random(2024);
            float min = float.MaxValue;
            float max = float.MinValue;
            for (int i = 0; i < 20000; i++)
            {
                float x = (float)(random.NextDouble() * 2000.0 - 1000.0);
                float y = (float)(random.NextDouble() * 2000.0 - 1000.0);
                float z = (float)(random.NextDouble() * 2000.0 - 1000.0);
                float a = WindNoise.Sample(x, y, z);
                float b = WindNoise.Sample(x, y, z);
                Assert.Equal(BitConverter.SingleToInt32Bits(a), BitConverter.SingleToInt32Bits(b));
                Assert.InRange(a, -1f, 1f);
                min = Math.Min(min, a);
                max = Math.Max(max, a);

                float u = WindNoise.Sample01(x, y);
                Assert.InRange(u, 0f, 1f);
            }

            Assert.True(max - min > 1.2f, "the noise should use most of its range, got [" + min + ", " + max + "]");
        }

        [Fact]
        public void Sample_IsSmoothAndPeriodic()
        {
            for (int i = 0; i < 1000; i++)
            {
                float x = i * 0.137f;
                float a = WindNoise.Sample(x, 3.3f, -7.1f);
                float b = WindNoise.Sample(x + 0.001f, 3.3f, -7.1f);
                Assert.True(Math.Abs(a - b) < 0.01f, "not continuous at x = " + x);
                float c = WindNoise.Sample(x + WindNoise.Period, 3.3f, -7.1f);
                Assert.True(Math.Abs(a - c) < 1e-3f, "not periodic at x = " + x);
            }
        }

        [Fact]
        public void Sample_HandlesNonFiniteInput()
        {
            Assert.Equal(0f, WindNoise.Sample(float.NaN, 0f, 0f));
            Assert.Equal(0f, WindNoise.Sample(0f, float.PositiveInfinity, 0f));
            Assert.Equal(0f, WindNoise.Sample(0f, 0f, 1e30f));
        }

        [Fact]
        public void SampleWind_ChannelsAreBoundedAndDecorrelated()
        {
            var random = new Random(5);
            double sumProduct = 0.0;
            const int count = 5000;
            for (int i = 0; i < count; i++)
            {
                var p = new V3((float)random.NextDouble() * 50f, (float)random.NextDouble() * 50f, (float)random.NextDouble() * 50f);
                float t = (float)random.NextDouble() * WindNoise.Period;
                WindNoise.SampleWind(p, t, out float gust, out V3 swirl);
                Assert.InRange(gust, -1f, 1f);
                Assert.InRange(swirl.x, -1f, 1f);
                Assert.InRange(swirl.y, -1f, 1f);
                Assert.InRange(swirl.z, -1f, 1f);
                sumProduct += gust * swirl.x;
            }

            Assert.InRange(sumProduct / count, -0.1, 0.1);
        }

        [Fact]
        public void WindWithTurbulence_MovesTheChainDeterministically()
        {
            V3 Run()
            {
                var solver = new ClothSolver();
                ClothParameters p = BuiltInPresets.GetOrDefault(BuiltInPresets.Ribbon);
                ChainScene scene = ChainScene.Straight(solver, new V3(0f, 1f, 0f), V3.Down, 6, 0.08f, p);
                solver.SetWind(scene.Group, new V3(4f, 0f, 0f), 0.7f, 1.5f);
                scene.Run(3f, 1f / 60f);
                return scene.Position(0, 5);
            }

            V3 a = Run();
            V3 b = Run();
            Assert.Equal(a, b);
            Assert.True(a.x > 0.05f, "wind along +X should blow the ribbon along +X: " + a);
        }
    }
}
