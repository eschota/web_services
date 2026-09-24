using System;

namespace AutoRig.Cloth.Core
{
    public sealed partial class ClothSolver
    {
        // |dot(q0, q1)| below cos(45°) means the rotation between the two frames exceeds 90°.
        private const float TeleportCosHalfAngle = 0.70710678f;

        /// <summary>
        /// Advances the simulation by <paramref name="deltaTime"/> seconds of frame time.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Call once per rendered frame, after <see cref="SetGroupFrame"/> and <see cref="SetTarget"/>
        /// have supplied this frame's animated pose. Time is consumed in fixed steps of
        /// <see cref="StepDeltaTime"/> (90 Hz by default) through an accumulator; the animated pose,
        /// the attach frames and the colliders are interpolated between the previous and the current
        /// frame for every step. At most <see cref="SolverSettings.maxSubsteps"/> steps run per call;
        /// beyond that the remaining time is dropped.
        /// </para>
        /// <para>
        /// Per group and step: inertia (the share of the attach frame's motion the cloth does not feel
        /// is applied rigidly), Verlet integration with damping, drag, gravity and wind, then
        /// <see cref="SolverSettings.iterations"/> constraint iterations (shape restoration, chain
        /// links with stretch allowance, angle limit, links between chains, collisions), and finally
        /// a follow-the-leader pass that restores exact link lengths root to tip and resolves
        /// collisions once more.
        /// </para>
        /// <para>
        /// A zero, negative or NaN <paramref name="deltaTime"/> (a paused game) does not step: free
        /// particles move rigidly with their attach frame so the cloth stays attached, and the
        /// output is refreshed.
        /// </para>
        /// </remarks>
        public void Simulate(float deltaTime)
        {
            float h = 1f / _settings.substepRate;
            for (int g = 0; g < _groupCount; g++)
            {
                BeginFrame(ref _groups[g], h);
            }

            if (!(deltaTime > 0f) || float.IsInfinity(deltaTime))
            {
                for (int g = 0; g < _groupCount; g++)
                {
                    FollowRigidly(ref _groups[g]);
                }

                EndColliderFrame();
                UpdateOutput();
                return;
            }

            float leftover = _accumulator;
            float available = leftover + deltaTime;
            int steps = (int)(available * _settings.substepRate + 1e-4f);
            bool capped = steps > _settings.maxSubsteps;
            if (capped)
            {
                steps = _settings.maxSubsteps;
            }

            for (int s = 0; s < steps; s++)
            {
                float alpha = capped ? (s + 1) / (float)steps : ((s + 1) * h - leftover) / deltaTime;
                alpha = alpha < 0f ? 0f : (alpha > 1f ? 1f : alpha);
                Array.Copy(_position, _stepStart, _particleCount);
                PrepareColliders(alpha);
                for (int g = 0; g < _groupCount; g++)
                {
                    Step(ref _groups[g], alpha, h);
                }

                _time += h;
            }

            _accumulator = capped ? 0f : available - steps * h;
            if (_accumulator < 0f)
            {
                _accumulator = 0f;
            }

            for (int g = 0; g < _groupCount; g++)
            {
                EndFrame(ref _groups[g]);
            }

            EndColliderFrame();
            UpdateOutput();
        }

        private void BeginFrame(ref GroupState g, float h)
        {
            bool reset = !g.initialized || g.resetRequested;
            if (!reset)
            {
                V3 move = g.frameCurrent.position - g.framePrevious.position;
                if (g.teleportDistance > 0f && move.LengthSquared > g.teleportDistance * g.teleportDistance)
                {
                    reset = true;
                }
                else if (Math.Abs(Q4.Dot(g.framePrevious.rotation, g.frameCurrent.rotation)) < TeleportCosHalfAngle)
                {
                    reset = true;
                }
            }

            if (reset)
            {
                ResetState(ref g);
            }

            // Link lengths come from the animated pose, so a scaled or re-proportioned character just works.
            int start = g.particleStart;
            int end = start + g.particleCount;
            float reach = 0f;
            float chainLength = 0f;
            for (int i = start; i < end; i++)
            {
                int p = _parent[i];
                if (p < 0)
                {
                    chainLength = 0f;
                    continue;
                }

                float rest = V3.Distance(_target[i], _target[p]);
                _restLength[i] = rest;
                chainLength += rest;
                if (chainLength > reach)
                {
                    reach = chainLength;
                }
            }

            int linkEnd = g.linkStart + g.linkCount;
            for (int l = g.linkStart; l < linkEnd; l++)
            {
                _linkRest[l] = V3.Distance(_target[_linkA[l]], _target[_linkB[l]]);
            }

            ClothParameters p0 = g.parameters;
            double frames = h * 60.0;
            double perIteration = frames / _settings.iterations;
            g.dampingKeep = (float)Math.Pow(1.0 - p0.damping, frames);
            g.stiffnessPerIteration = 1f - (float)Math.Pow(1.0 - p0.stiffness, perIteration);
            g.connectionPerIteration = 1f - (float)Math.Pow(1.0 - p0.connectionStiffness, perIteration);
            g.angleLimited = p0.angleLimitDeg > 0f && p0.angleLimitDeg < 180f;
            double limit = p0.angleLimitDeg * (Math.PI / 180.0);
            g.cosLimit = (float)Math.Cos(limit);
            g.sinLimit = (float)Math.Sin(limit);
            g.stretchMin = 1f - p0.stretch;
            g.stretchMax = 1f + p0.stretch;
            g.dragCoefficient = p0.drag * DragCoefficientScale / g.lengthScale;
            g.radiusRoot = p0.radius * g.lengthScale;
            g.radiusTip = p0.EffectiveRadiusTip * g.lengthScale;
            g.reach = reach * g.stretchMax;
            g.translateShare = 1f - p0.inertiaMove;
            g.rotateShare = 1f - p0.inertiaRotate;
            g.gravity = _settings.gravity * p0.gravity;
        }

        private void ResetState(ref GroupState g)
        {
            int end = g.particleStart + g.particleCount;
            for (int i = g.particleStart; i < end; i++)
            {
                V3 t = _target[i];
                _position[i] = t;
                _previous[i] = t;
                _stepStart[i] = t;
                _targetPrevious[i] = t;
                _stepTarget[i] = t;
                _output[i] = t;
            }

            g.framePrevious = g.frameCurrent;
            g.frameStep = g.frameCurrent;
            g.initialized = true;
            g.resetRequested = false;
        }

        private void Step(ref GroupState g, float alpha, float h)
        {
            int start = g.particleStart;
            int end = start + g.particleCount;

            for (int i = start; i < end; i++)
            {
                _stepTarget[i] = V3.Lerp(_targetPrevious[i], _target[i], alpha);
            }

            for (int i = start; i < end; i++)
            {
                int p = _parent[i];
                if (p >= 0)
                {
                    _animDirection[i] = V3.NormalizeOr(_stepTarget[i] - _stepTarget[p], _animDirection[i]);
                }
            }

            RigidFrame frame = RigidFrame.Interpolate(g.framePrevious, g.frameCurrent, alpha);
            ApplyInertia(ref g, frame);
            g.frameStep = frame;

            Integrate(ref g, h);

            int candidates = GatherColliders(ref g);
            for (int iteration = 0; iteration < _settings.iterations; iteration++)
            {
                SolveConstraints(ref g, candidates);
            }

            FollowTheLeader(ref g, candidates);
        }

        // Applies the part of the attach frame's motion that the cloth should not feel as inertia,
        // rigidly, to the current and previous positions of the free particles (velocity is kept
        // relative to the partially moving frame).
        private void ApplyInertia(ref GroupState g, RigidFrame frame)
        {
            float move = g.translateShare;
            float turn = g.rotateShare;
            if (move <= 0f && turn <= 0f)
            {
                return;
            }

            V3 pivot = g.frameStep.position;
            V3 translation = move > 0f ? (frame.position - pivot) * move : V3.Zero;
            Q4 rotation = Q4.Identity;
            bool rotate = false;
            if (turn > 0f)
            {
                Q4 delta = frame.rotation * Q4.Inverse(g.frameStep.rotation);
                rotation = Q4.Slerp(Q4.Identity, delta, turn);
                rotate = Math.Abs(rotation.w) < 1f;
            }

            if (!rotate && translation.LengthSquared == 0f)
            {
                return;
            }

            int end = g.particleStart + g.particleCount;
            for (int i = g.particleStart; i < end; i++)
            {
                if (_inverseMass[i] == 0f)
                {
                    continue;
                }

                if (rotate)
                {
                    _position[i] = pivot + translation + rotation * (_position[i] - pivot);
                    _previous[i] = pivot + translation + rotation * (_previous[i] - pivot);
                }
                else
                {
                    _position[i] += translation;
                    _previous[i] += translation;
                }
            }
        }

        private void Integrate(ref GroupState g, float h)
        {
            float hh = h * h;
            V3 gravityStep = g.gravity * hh;
            V3 windBase = g.windAcceleration * g.parameters.wind;
            bool windy = windBase.LengthSquared > 0f;
            float windMagnitude = windy ? windBase.Length : 0f;
            float noiseTime = (float)((_time * g.windFrequency) % WindNoise.Period);
            V3 chainWind = V3.Zero;
            float keep = g.dampingKeep;
            float drag = g.dragCoefficient;

            int end = g.particleStart + g.particleCount;
            for (int i = g.particleStart; i < end; i++)
            {
                if (_inverseMass[i] == 0f)
                {
                    _previous[i] = _position[i];
                    _position[i] = _stepTarget[i];
                    if (windy)
                    {
                        chainWind = ChainWind(windBase, windMagnitude, g.windTurbulence, _stepTarget[i], noiseTime) * hh;
                    }

                    continue;
                }

                V3 velocity = (_position[i] - _previous[i]) * keep;
                if (drag > 0f)
                {
                    velocity *= 1f / (1f + drag * velocity.Length);
                }

                _previous[i] = _position[i];
                _position[i] = _position[i] + velocity + gravityStep + chainWind;
            }
        }

        // Wind for one chain, sampled at its root: the mean wind scaled by a gust factor plus a swirl.
        private static V3 ChainWind(V3 windBase, float magnitude, float turbulence, V3 at, float noiseTime)
        {
            if (turbulence <= 0f)
            {
                return windBase;
            }

            WindNoise.SampleWind(at, noiseTime, out float gust, out V3 swirl);
            return windBase * (1f + turbulence * gust) + swirl * (magnitude * turbulence * 0.5f);
        }

        // Broad phase: keeps the colliders whose tag matches the group and whose bounds overlap the
        // region the group can reach this step (roots' bounds grown by the longest chain).
        private int GatherColliders(ref GroupState g)
        {
            if (_colliderCount == 0)
            {
                return 0;
            }

            var min = new V3(float.MaxValue, float.MaxValue, float.MaxValue);
            var max = new V3(float.MinValue, float.MinValue, float.MinValue);
            int end = g.particleStart + g.particleCount;
            for (int i = g.particleStart; i < end; i++)
            {
                if (_parent[i] < 0)
                {
                    min = V3.Min(min, _stepTarget[i]);
                    max = V3.Max(max, _stepTarget[i]);
                }
            }

            float margin = g.reach + Math.Max(g.radiusRoot, g.radiusTip);
            var grow = new V3(margin, margin, margin);
            min -= grow;
            max += grow;

            int count = 0;
            for (int c = 0; c < _colliderCount; c++)
            {
                ref ColliderState col = ref _colliders[c];
                if (!col.enabled || !col.valid || (col.tags & g.colliderMask) == 0u)
                {
                    continue;
                }

                if (col.shape != ColliderShape.Plane
                    && (col.boundsMax.x < min.x || col.boundsMin.x > max.x
                        || col.boundsMax.y < min.y || col.boundsMin.y > max.y
                        || col.boundsMax.z < min.z || col.boundsMin.z > max.z))
                {
                    continue;
                }

                _candidates[count++] = c;
            }

            return count;
        }

        private void SolveConstraints(ref GroupState g, int candidateCount)
        {
            int start = g.particleStart;
            int end = start + g.particleCount;
            float stiffness = g.stiffnessPerIteration;
            float stretchMin = g.stretchMin;
            float stretchMax = g.stretchMax;

            for (int i = start; i < end; i++)
            {
                float wi = _inverseMass[i];
                if (wi == 0f)
                {
                    continue;
                }

                int p = _parent[i];
                V3 axis = _animDirection[i];
                float rest = _restLength[i];
                V3 pi = _position[i];
                V3 pp = _position[p];

                // Shape restoration: turn the link toward its animated direction. The goal keeps the
                // link's current length (the length constraint just below restores it), so the two
                // never fight. Most of the correction is applied to the previous position as well, so
                // it does not turn into velocity: the chain returns to its pose with a soft bounce
                // instead of buzzing like a stiff spring.
                if (stiffness > 0f)
                {
                    V3 correction = (pp + axis * (pi - pp).Length - pi) * stiffness;
                    pi += correction;
                    _previous[i] += correction * RestorationVelocityAttenuation;
                }

                // Link to the parent: hard, within the stretch allowance.
                V3 d = pi - pp;
                float length = d.Length;
                if (length > Epsilon)
                {
                    float lo = rest * stretchMin;
                    float hi = rest * stretchMax;
                    float clamped = length < lo ? lo : (length > hi ? hi : length);
                    if (clamped != length)
                    {
                        float wp = _inverseMass[p];
                        float k = (length - clamped) / (length * (wi + wp));
                        pi -= d * (k * wi);
                        if (wp > 0f)
                        {
                            pp += d * (k * wp);
                            _position[p] = pp;
                        }
                    }
                }

                // Angle limit relative to the animated direction.
                if (g.angleLimited)
                {
                    V3 e = pi - pp;
                    float l = e.Length;
                    if (l > Epsilon)
                    {
                        V3 dir = e * (1f / l);
                        if (V3.Dot(dir, axis) < g.cosLimit)
                        {
                            pi = pp + ClothGeometry.ClampToCone(dir, axis, g.cosLimit, g.sinLimit) * l;
                        }
                    }
                }

                _position[i] = pi;
            }

            // Links between neighbouring chains.
            float connection = g.connectionPerIteration;
            if (g.linkCount > 0 && connection > 0f)
            {
                int linkEnd = g.linkStart + g.linkCount;
                for (int l = g.linkStart; l < linkEnd; l++)
                {
                    int a = _linkA[l];
                    int b = _linkB[l];
                    float wa = _inverseMass[a];
                    float wb = _inverseMass[b];
                    float w = wa + wb;
                    if (w <= 0f)
                    {
                        continue;
                    }

                    V3 d = _position[b] - _position[a];
                    float length = d.Length;
                    if (length <= Epsilon)
                    {
                        continue;
                    }

                    float s = (length - _linkRest[l]) / (length * w) * connection;
                    _position[a] += d * (s * wa);
                    _position[b] -= d * (s * wb);
                }
            }

            if (candidateCount > 0)
            {
                for (int i = start; i < end; i++)
                {
                    if (_inverseMass[i] == 0f)
                    {
                        continue;
                    }

                    V3 pi = _position[i];
                    Collide(ref pi, g.radiusRoot + (g.radiusTip - g.radiusRoot) * _depth[i], candidateCount);
                    _position[i] = pi;
                }
            }
        }

        // Root to tip: re-imposes each link length (exact rest length, or the nearest length inside
        // the stretch allowance) and the angle limit by moving only the child, then pushes that child
        // out of colliders. Collision wins over length here: a pushed joint may end up to its push
        // distance away from its rest length, but its children are placed from its corrected position,
        // so the error never accumulates along the chain.
        private void FollowTheLeader(ref GroupState g, int candidateCount)
        {
            int start = g.particleStart;
            int end = start + g.particleCount;
            for (int i = start; i < end; i++)
            {
                if (_inverseMass[i] == 0f)
                {
                    continue;
                }

                int p = _parent[i];
                V3 axis = _animDirection[i];
                float rest = _restLength[i];
                V3 pp = _position[p];
                V3 d = _position[i] - pp;
                float length = d.Length;
                V3 dir = length > Epsilon ? d * (1f / length) : axis;
                if (g.angleLimited && V3.Dot(dir, axis) < g.cosLimit)
                {
                    dir = ClothGeometry.ClampToCone(dir, axis, g.cosLimit, g.sinLimit);
                }

                float lo = rest * g.stretchMin;
                float hi = rest * g.stretchMax;
                float clamped = length < lo ? lo : (length > hi ? hi : length);
                V3 pi = pp + dir * clamped;
                if (candidateCount > 0)
                {
                    Collide(ref pi, g.radiusRoot + (g.radiusTip - g.radiusRoot) * _depth[i], candidateCount);
                }

                _position[i] = pi;
            }
        }

        private void Collide(ref V3 p, float radius, int candidateCount)
        {
            for (int k = 0; k < candidateCount; k++)
            {
                ref ColliderState c = ref _colliders[_candidates[k]];
                switch (c.shape)
                {
                    case ColliderShape.Sphere:
                        ClothGeometry.PushOutOfSphere(ref p, radius, c.aStep, c.raStep);
                        break;
                    case ColliderShape.Capsule:
                        ClothGeometry.PushOutOfCapsule(ref p, radius, c.aStep, c.bStep, c.raStep, c.rbStep);
                        break;
                    default:
                        ClothGeometry.PushOutOfPlane(ref p, radius, c.aStep, c.bStep);
                        break;
                }
            }
        }

        private void PrepareColliders(float alpha)
        {
            for (int c = 0; c < _colliderCount; c++)
            {
                ref ColliderState col = ref _colliders[c];
                if (!col.valid || !col.enabled)
                {
                    continue;
                }

                if (!col.hasPrevious)
                {
                    col.aPrevious = col.a;
                    col.bPrevious = col.b;
                    col.raPrevious = col.ra;
                    col.rbPrevious = col.rb;
                    col.hasPrevious = true;
                }

                col.aStep = V3.Lerp(col.aPrevious, col.a, alpha);
                col.bStep = V3.Lerp(col.bPrevious, col.b, alpha);
                col.raStep = col.raPrevious + (col.ra - col.raPrevious) * alpha;
                col.rbStep = col.rbPrevious + (col.rb - col.rbPrevious) * alpha;
                if (col.shape == ColliderShape.Plane)
                {
                    col.bStep = V3.NormalizeOr(col.bStep, col.b);
                }
                else
                {
                    float r = Math.Max(col.raStep, col.rbStep);
                    var grow = new V3(r, r, r);
                    col.boundsMin = V3.Min(col.aStep, col.bStep) - grow;
                    col.boundsMax = V3.Max(col.aStep, col.bStep) + grow;
                }
            }
        }

        private void EndColliderFrame()
        {
            for (int c = 0; c < _colliderCount; c++)
            {
                ref ColliderState col = ref _colliders[c];
                if (!col.valid)
                {
                    continue;
                }

                col.aPrevious = col.a;
                col.bPrevious = col.b;
                col.raPrevious = col.ra;
                col.rbPrevious = col.rb;
                col.hasPrevious = true;
            }
        }

        private void EndFrame(ref GroupState g)
        {
            bool finite = true;
            int end = g.particleStart + g.particleCount;
            for (int i = g.particleStart; i < end; i++)
            {
                _targetPrevious[i] = _target[i];
                if (!_position[i].IsFinite)
                {
                    finite = false;
                }
            }

            g.framePrevious = g.frameCurrent;
            if (!finite)
            {
                ResetState(ref g); // never let a bad input poison the state
            }
        }

        // Paused frame: no simulation, but the free particles follow their attach frame rigidly so
        // the cloth stays attached when a paused character is moved.
        private void FollowRigidly(ref GroupState g)
        {
            RigidFrame from = g.framePrevious;
            RigidFrame to = g.frameCurrent;
            bool moved = from.position != to.position || !from.rotation.Equals(to.rotation);
            Q4 delta = Q4.Normalize(to.rotation * Q4.Inverse(from.rotation));
            int end = g.particleStart + g.particleCount;
            for (int i = g.particleStart; i < end; i++)
            {
                _targetPrevious[i] = _target[i];
                if (_inverseMass[i] == 0f)
                {
                    _position[i] = _target[i];
                    _previous[i] = _target[i];
                    _stepStart[i] = _target[i];
                }
                else if (moved)
                {
                    _position[i] = to.position + delta * (_position[i] - from.position);
                    _previous[i] = to.position + delta * (_previous[i] - from.position);
                    _stepStart[i] = to.position + delta * (_stepStart[i] - from.position);
                }
            }

            if (moved)
            {
                g.frameStep = new RigidFrame(
                    to.position + delta * (g.frameStep.position - from.position),
                    Q4.Normalize(delta * g.frameStep.rotation));
            }

            g.framePrevious = to;
        }

        // Render positions: the state interpolated between the last two steps (the display lags the
        // simulation by at most one step but moves smoothly at any frame rate), with every chain
        // translated so its root sits exactly on its current animated position.
        private void UpdateOutput()
        {
            float alpha = _accumulator * _settings.substepRate;
            alpha = alpha < 0f ? 0f : (alpha > 1f ? 1f : alpha);
            for (int g = 0; g < _groupCount; g++)
            {
                int start = _groups[g].particleStart;
                int end = start + _groups[g].particleCount;
                V3 offset = V3.Zero;
                for (int i = start; i < end; i++)
                {
                    V3 s = V3.Lerp(_stepStart[i], _position[i], alpha);
                    if (_parent[i] < 0)
                    {
                        offset = _target[i] - s;
                        _output[i] = _target[i];
                    }
                    else
                    {
                        _output[i] = s + offset;
                    }
                }
            }
        }
    }
}
