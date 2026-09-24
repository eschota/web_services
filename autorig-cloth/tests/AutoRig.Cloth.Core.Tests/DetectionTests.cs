using System;
using System.Collections.Generic;
using AutoRig.Cloth.Core;
using Xunit;

namespace AutoRig.Cloth.Core.Tests
{
    public class DetectionTests
    {
        [Theory]
        [InlineData("hair_back_00_0", ChainCategory.Hair)]
        [InlineData("J_Sec_Hair1_01", ChainCategory.Hair)]
        [InlineData("mixamorig:Hair01", ChainCategory.Hair)]
        [InlineData("PonyTail_1", ChainCategory.Hair)]
        [InlineData("TwinTail_L_01", ChainCategory.Hair)]
        [InlineData("braid_02", ChainCategory.Hair)]
        [InlineData("Bangs_01", ChainCategory.Bangs)]
        [InlineData("hair_bang_L", ChainCategory.Bangs)]
        [InlineData("J_Sec_L_SkirtBack0_01", ChainCategory.Skirt)]
        [InlineData("skirt_00_0", ChainCategory.Skirt)]
        [InlineData("Cape_01", ChainCategory.Cape)]
        [InlineData("CloakL1", ChainCategory.Cape)]
        [InlineData("CoatTail_L", ChainCategory.Coat)]
        [InlineData("coat_02", ChainCategory.Coat)]
        [InlineData("Tail_01", ChainCategory.Tail)]
        [InlineData("tail1", ChainCategory.Tail)]
        [InlineData("Ribbon_R_1", ChainCategory.Ribbon)]
        [InlineData("scarf_end_2", ChainCategory.Scarf)]
        [InlineData("Sleeve_L_01", ChainCategory.Sleeve)]
        [InlineData("Ear_L", ChainCategory.Ear)]
        [InlineData("LeftEar", ChainCategory.Ear)]
        [InlineData("ears_01", ChainCategory.Ear)]
        [InlineData("mixamorig:LeftForeArm", ChainCategory.None)]
        [InlineData("Earring_L", ChainCategory.None)]
        [InlineData("Detail_01", ChainCategory.None)]
        [InlineData("Breast_L", ChainCategory.None)]
        [InlineData("HeadTop_End", ChainCategory.None)]
        [InlineData("Chair_01", ChainCategory.None)]
        [InlineData("Hips", ChainCategory.None)]
        [InlineData("", ChainCategory.None)]
        public void Classify_ByName(string name, ChainCategory expected)
        {
            Assert.Equal(expected, ChainKeywords.Classify(name));
        }

        [Fact]
        public void Tokenize_SplitsSeparatorsDigitsAndCamelCase()
        {
            Assert.Equal(new[] { "left", "fore", "arm" }, ChainKeywords.Tokenize("LeftForeArm"));
            Assert.Equal(new[] { "j", "sec", "hair", "1", "01" }, ChainKeywords.Tokenize("J_Sec_Hair1_01"));
            Assert.Equal(new[] { "head", "top", "end" }, ChainKeywords.Tokenize("HeadTop_End"));
        }

        [Fact]
        public void Profiles_MapCategoriesToKindsPresetsAndConnections()
        {
            Assert.Equal("hair", ChainKeywords.KindOf(ChainCategory.Bangs));
            Assert.Equal("cloth", ChainKeywords.KindOf(ChainCategory.Skirt));
            Assert.Equal("tail", ChainKeywords.KindOf(ChainCategory.Tail));
            Assert.Equal("accessory", ChainKeywords.KindOf(ChainCategory.Ear));
            Assert.Equal(BuiltInPresets.HairStiff, ChainKeywords.PresetOf(ChainCategory.Bangs));
            Assert.Equal(BuiltInPresets.Cape, ChainKeywords.PresetOf(ChainCategory.Cape));

            Assert.Equal(ConnectionMode.Loop, ChainKeywords.ConnectionFor(ChainCategory.Skirt, 8, 45f));
            Assert.Equal(ConnectionMode.Open, ChainKeywords.ConnectionFor(ChainCategory.Skirt, 6, 150f));
            Assert.Equal(ConnectionMode.Open, ChainKeywords.ConnectionFor(ChainCategory.Cape, 5, 280f));
            Assert.Equal(ConnectionMode.None, ChainKeywords.ConnectionFor(ChainCategory.Hair, 12, 30f));
            Assert.Equal(ConnectionMode.None, ChainKeywords.ConnectionFor(ChainCategory.Skirt, 1, 360f));
        }

        [Fact]
        public void Ring_OrdersAFullSkirtByAngle()
        {
            // Eight roots around the hips, supplied shuffled.
            int[] shuffle = { 5, 2, 7, 0, 3, 6, 1, 4 };
            var roots = new List<V3>();
            foreach (int k in shuffle)
            {
                double a = 2.0 * Math.PI * k / 8.0;
                roots.Add(new V3((float)Math.Cos(a) * 0.2f, 1f, (float)Math.Sin(a) * 0.2f));
            }

            int[] order = ChainRing.OrderAround(roots, new V3(0f, 1.05f, 0f), V3.Down, out float gap);
            Assert.InRange(gap, 44.9f, 45.1f);
            Assert.Equal(ConnectionMode.Loop, ChainKeywords.ConnectionFor(ChainCategory.Skirt, 8, gap));

            // Consecutive entries are angular neighbours, all the way round.
            for (int i = 0; i < 8; i++)
            {
                int a = shuffle[order[i]];
                int b = shuffle[order[(i + 1) % 8]];
                Assert.Equal(1, Math.Min((b - a + 8) % 8, (a - b + 8) % 8));
            }
        }

        [Fact]
        public void Ring_PutsTheGapOfAnOpenCapeAtTheEnds()
        {
            // Five cape roots across the upper back (behind the attach bone, spanning ±60°).
            var roots = new List<V3>();
            int[] shuffle = { 3, 0, 4, 1, 2 };
            foreach (int k in shuffle)
            {
                double a = (-60.0 + 30.0 * k) * Math.PI / 180.0;
                roots.Add(new V3((float)Math.Sin(a) * 0.15f, 1.4f, -(float)Math.Cos(a) * 0.15f));
            }

            int[] order = ChainRing.OrderAround(roots, new V3(0f, 1.4f, 0f), V3.Down, out float gap);
            Assert.InRange(gap, 239f, 241f);
            var sequence = new int[5];
            for (int i = 0; i < 5; i++)
            {
                sequence[i] = shuffle[order[i]];
            }

            // Either direction is fine, but the ends must be the outermost chains.
            Assert.True(
                sequence[0] == 0 && sequence[4] == 4 || sequence[0] == 4 && sequence[4] == 0,
                "order was " + string.Join(",", sequence));
            for (int i = 1; i < 5; i++)
            {
                Assert.Equal(1, Math.Abs(sequence[i] - sequence[i - 1]));
            }
        }

        [Fact]
        public void Ring_HandlesDegenerateInput()
        {
            Assert.Empty(ChainRing.OrderAround(new List<V3>(), V3.Zero, V3.Down, out float g0));
            Assert.Equal(360f, g0);
            Assert.Equal(new[] { 0 }, ChainRing.OrderAround(new List<V3> { V3.Up }, V3.Zero, V3.Down, out _));

            // All roots at the centre: stable order, no NaN.
            int[] order = ChainRing.OrderAround(new List<V3> { V3.Zero, V3.Zero, V3.Zero }, V3.Zero, V3.Down, out float gap);
            Assert.Equal(3, order.Length);
            Assert.False(float.IsNaN(gap));
        }
    }
}
