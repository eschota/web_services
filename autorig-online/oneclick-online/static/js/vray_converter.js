/**
 * VRayConverter — converts VRay scene JSON into Three.js PBR materials,
 * lights, and camera definitions for the task viewer.
 *
 * JSON format: { "objs": [ ... ] }
 * Material binding: mat_{meshidx}_{submtl}_mode_{N} -> mesh with matching meshidx
 *
 * CONSTRAINTS (WebGL-safe):
 *   - MeshStandardMaterial only (not Physical) — fewer texture slots
 *   - Max 3 texture maps per material: map, normalMap, emissiveMap
 *   - Max 1 DirectionalLight + 3 PointLights (no shadow on point lights)
 */

export class VRayConverter {
    constructor(taskId, THREE) {
        this.taskId = taskId;
        this.THREE = THREE;
        this.rawObjs = [];

        this.materials = {};        // ocname -> VRayMtl params
        this.meshidxToMtls = {};    // meshidx -> [{submtl, obj}]
        this.meshObjects = [];      // Editable_mesh / PolyMeshObject
        this.lights = [];           // [{type, name, params}]
        this.cameras = [];          // [{name, params}]
        this.sceneInfo = null;

        this._textureLoader = new THREE.TextureLoader();
        this._textureCache = new Map();
    }

    /* ─── Loading & parsing ─── */

    async loadScene(jsonUrl) {
        const resp = await fetch(jsonUrl);
        if (!resp.ok) throw new Error(`Scene JSON fetch failed: ${resp.status}`);
        let text = await resp.text();
        if (!text || text.length < 10) throw new Error('Scene JSON is empty');
        if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1); // BOM

        let parsed;
        try {
            parsed = JSON.parse(text);
        } catch (e) {
            // Worker may send truncated JSON while task is still processing.
            // Try to repair by finding the last complete object in the "objs" array.
            console.warn('[VRayConverter] JSON parse failed, attempting repair...', e.message);
            parsed = this._repairTruncatedJson(text);
        }
        this._parseObjects(parsed);
        return this;
    }

    /** Try to repair truncated JSON of the form {"objs": [{...}, {..TRUNCATED */
    _repairTruncatedJson(text) {
        // Find pattern: last occurrence of "},\n" or "}\n" that closes a complete object
        // in the objs array. Then close the array and root object.
        // Strategy: search backwards for "},\n{" or just "}" + end of an object boundary.
        let lastGoodEnd = -1;

        // Look for the last "},\n" which ends a complete obj followed by the next one
        const re = /\}\s*,\s*\n\s*\{/g;
        let m;
        while ((m = re.exec(text)) !== null) {
            // m.index points to "}" — the end of a complete object
            lastGoodEnd = m.index + 1; // include the "}"
        }

        if (lastGoodEnd === -1) {
            // Try simpler: last "}" followed by anything
            const lastBrace = text.lastIndexOf('}');
            if (lastBrace > 0) lastGoodEnd = lastBrace + 1;
        }

        if (lastGoodEnd > 0) {
            // Check if we're inside the "objs" array
            const prefix = text.slice(0, lastGoodEnd);
            // Close the array and root object
            const repaired = prefix + ']}';
            try {
                const obj = JSON.parse(repaired);
                console.log(`[VRayConverter] Repaired truncated JSON: ${lastGoodEnd}/${text.length} bytes used`);
                return obj;
            } catch (e2) {
                console.warn('[VRayConverter] Repair attempt 1 failed:', e2.message);
            }
        }

        // Last resort: extract individual complete objects using regex
        const objs = [];
        const objRe = /\{\s*"idx"\s*:\s*\d+[^{}]*(?:\{[^{}]*\}[^{}]*)*\}/g;
        let match;
        while ((match = objRe.exec(text)) !== null) {
            try {
                objs.push(JSON.parse(match[0]));
            } catch (_) {
                // skip malformed entries
            }
        }
        if (objs.length > 0) {
            console.log(`[VRayConverter] Recovered ${objs.length} objects via regex extraction`);
            return { objs };
        }

        throw new Error('Scene JSON is truncated and could not be repaired');
    }

    _parseObjects(json) {
        const objs = Array.isArray(json) ? json
            : (json && Array.isArray(json.objs)) ? json.objs : [];
        this.rawObjs = objs;

        for (const obj of objs) {
            if (!obj || typeof obj !== 'object') continue;
            const t = obj.octype || '';
            if (t === 'VRayMtl' || t === 'VRayLightMtl') {
                this.materials[obj.ocname] = obj;
                this._indexMaterial(obj);
            } else if (t === 'Editable_mesh' || t === 'PolyMeshObject') {
                this.meshObjects.push(obj);
            } else if (t === 'VRaySun') {
                this.lights.push({ type: 'VRaySun', name: obj.ocname, params: obj });
            } else if (t === 'VRayLight') {
                this.lights.push({ type: 'VRayLight', name: obj.ocname, params: obj });
            } else if (t === 'VRayPhysicalCamera') {
                this.cameras.push({ name: obj.ocname, params: obj });
            } else if (t === 'fake') {
                this.sceneInfo = obj;
            }
        }
        console.log(`[VRayConverter] Parsed: ${Object.keys(this.materials).length} materials, ` +
            `${this.meshObjects.length} meshes, ${this.lights.length} lights, ${this.cameras.length} cameras`);
    }

    _indexMaterial(obj) {
        const m = (obj.ocname || '').match(/^mat_(\d+)_(\d+)_mode_(\d+)$/);
        if (!m) return;
        const meshidx = +m[1], submtl = +m[2];
        (this.meshidxToMtls[meshidx] ||= []).push({ submtl, obj });
        this.meshidxToMtls[meshidx].sort((a, b) => a.submtl - b.submtl);
    }

    /* ─── Textures ─── */

    resolveTextureUrl(winPath) {
        if (!winPath || typeof winPath !== 'string') return null;
        const name = winPath.replace(/\\/g, '/').split('/').pop();
        return name ? `/api/task/${this.taskId}/textures/${encodeURIComponent(name)}` : null;
    }

    _loadTex(url, srgb) {
        if (!url) return null;
        const key = srgb ? url : url + '__lin';
        if (this._textureCache.has(key)) return this._textureCache.get(key);
        const tex = this._textureLoader.load(url);
        tex.colorSpace = srgb ? this.THREE.SRGBColorSpace : this.THREE.LinearSRGBColorSpace;
        tex.wrapS = tex.wrapT = this.THREE.RepeatWrapping;
        this._textureCache.set(key, tex);
        return tex;
    }

    /* ─── Material conversion (MeshStandardMaterial, max 3 tex) ─── */

    convertMaterial(vray) {
        if (!vray) return null;
        const THREE = this.THREE;
        if ((vray.octype || '') === 'VRayLightMtl') return this._convertLightMtl(vray);

        const params = { metalness: 0.0, roughness: 0.5 };
        let texCount = 0;
        const MAX_TEX = 3; // stay well under WebGL limit

        // Diffuse color
        const diffuse = vray.Diffuse || vray.diffuse;
        if (Array.isArray(diffuse) && diffuse.length >= 3) {
            const d = (diffuse[3] > 1) ? diffuse[3] : 255;
            params.color = new THREE.Color(diffuse[0] / d, diffuse[1] / d, diffuse[2] / d);
        }

        // Diffuse texture (priority 1)
        if (texCount < MAX_TEX && vray.texmap_diffuse && typeof vray.texmap_diffuse === 'string') {
            const url = this.resolveTextureUrl(vray.texmap_diffuse);
            if (url) { params.map = this._loadTex(url, true); texCount++; }
        }

        // Metalness (value only, no map)
        if (vray.reflection_metalness !== undefined)
            params.metalness = parseFloat(vray.reflection_metalness) || 0;

        // Roughness (value only — skip roughnessMap to save texture slots)
        const glossiness = vray.reflection_glossiness;
        if (glossiness !== undefined) {
            const g = parseFloat(glossiness);
            params.roughness = vray.brdf_useRoughness ? g : (1.0 - g);
        }

        // Normal map (priority 2)
        if (texCount < MAX_TEX && vray.texmap_bump && typeof vray.texmap_bump === 'string') {
            const url = this.resolveTextureUrl(vray.texmap_bump);
            if (url) {
                const isNormal = /_nrm[._]|_normal[._]/i.test(url);
                if (isNormal) {
                    params.normalMap = this._loadTex(url, false);
                    const a = parseFloat(vray.bump_amt);
                    if (!isNaN(a)) params.normalScale = new THREE.Vector2(a, a);
                } else {
                    params.bumpMap = this._loadTex(url, false);
                    const a = parseFloat(vray.bump_amt);
                    if (!isNaN(a)) params.bumpScale = a;
                }
                texCount++;
            }
        }

        // Emissive map (priority 3)
        if (texCount < MAX_TEX && vray.texmap_self_illumination && typeof vray.texmap_self_illumination === 'string') {
            const url = this.resolveTextureUrl(vray.texmap_self_illumination);
            if (url) {
                params.emissiveMap = this._loadTex(url, true);
                params.emissive = new THREE.Color(1, 1, 1);
                params.emissiveIntensity = Math.min(parseFloat(vray.selfIllumination_multiplier) || 1, 3);
                texCount++;
            }
        } else {
            // Emissive color only (no texture slot used)
            const si = vray.selfIllumination;
            if (Array.isArray(si) && si.length >= 3) {
                const d = (si[3] > 1) ? si[3] : 255;
                const r = si[0] / d, g = si[1] / d, b = si[2] / d;
                if (r > 0.01 || g > 0.01 || b > 0.01) {
                    params.emissive = new THREE.Color(r, g, b);
                    params.emissiveIntensity = Math.min(parseFloat(vray.selfIllumination_multiplier) || 1, 3);
                }
            }
        }

        // Opacity via alphaMap — only if we have budget AND it's needed
        if (texCount < MAX_TEX && vray.texmap_opacity && typeof vray.texmap_opacity === 'string') {
            const url = this.resolveTextureUrl(vray.texmap_opacity);
            if (url) {
                params.alphaMap = this._loadTex(url, false);
                params.transparent = true;
                params.alphaTest = 0.5;
                params.depthWrite = true;
                texCount++;
            }
        }

        // Double-sided
        if (vray.DoubleSided === true || vray.DoubleSided === 1)
            params.side = THREE.DoubleSide;

        return new THREE.MeshStandardMaterial(params);
    }

    _convertLightMtl(vray) {
        const THREE = this.THREE;
        const params = { metalness: 0, roughness: 1, color: new THREE.Color(0, 0, 0) };
        const c = vray.color || vray.Color;
        if (Array.isArray(c) && c.length >= 3) {
            const d = (c[3] > 1) ? c[3] : 255;
            params.emissive = new THREE.Color(c[0] / d, c[1] / d, c[2] / d);
        } else {
            params.emissive = new THREE.Color(1, 1, 1);
        }
        params.emissiveIntensity = Math.min(parseFloat(vray.multiplier) || 1, 3);
        if (vray.DoubleSided) params.side = THREE.DoubleSide;
        return new THREE.MeshStandardMaterial(params);
    }

    /* ─── Apply materials ─── */

    applyMaterials(model) {
        if (!Object.keys(this.materials).length) return;
        const THREE = this.THREE;

        // name -> meshidx lookup
        const nameToMi = new Map();
        for (const mObj of this.meshObjects) {
            const mi = mObj.meshidx;
            if (mi == null) continue;
            if (mObj.ocname) nameToMi.set(mObj.ocname.toLowerCase(), mi);
            if (mObj.meshname) nameToMi.set(mObj.meshname.toLowerCase(), mi);
        }

        const mtlCache = new Map();
        const getMtl = (meshidx) => {
            if (mtlCache.has(meshidx)) return mtlCache.get(meshidx);
            const defs = this.meshidxToMtls[meshidx];
            if (!defs?.length) { mtlCache.set(meshidx, null); return null; }
            const result = defs.length === 1
                ? this.convertMaterial(defs[0].obj)
                : defs.map(d => this.convertMaterial(d.obj));
            mtlCache.set(meshidx, result);
            return result;
        };

        let applied = 0, total = 0;
        const gray = new THREE.MeshStandardMaterial({ color: 0x808080, metalness: 0.15, roughness: 0.85 });

        model.traverse((child) => {
            if (!child.isMesh) return;
            total++;

            const n = (child.name || '').toLowerCase();
            let mi = nameToMi.get(n);
            if (mi === undefined) mi = nameToMi.get(n.replace(/_?\d+$/, ''));

            // Try FBX embedded material name
            if (mi === undefined && child.material) {
                const mn = (Array.isArray(child.material) ? child.material[0]?.name : child.material.name || '').toLowerCase();
                if (mn) mi = nameToMi.get(mn);
            }

            if (mi !== undefined) {
                const mtl = getMtl(mi);
                if (mtl) {
                    child.material = Array.isArray(mtl)
                        ? (child.geometry?.groups?.length > 0 ? mtl : mtl[0])
                        : mtl;
                    applied++;
                } else {
                    child.material = gray;
                }
            } else {
                child.material = gray;
            }
            child.castShadow = true;
            child.receiveShadow = true;
        });

        console.log(`[VRayConverter] Applied materials: ${applied}/${total} meshes`);
    }

    /* ─── FBX transform extraction ─── */

    /**
     * Build a lookup of name -> { worldPos, worldQuat } from loaded FBX model.
     * FBXLoader already applies correct coordinate transforms (Z-up -> Y-up),
     * so these are the authoritative positions for cameras, lights, targets, etc.
     * Must be called after FBX model is loaded and added to the scene (or at least
     * after model.updateWorldMatrix(true, true)).
     */
    extractFbxTransforms(model) {
        const THREE = this.THREE;
        this._fbxNodes = new Map(); // lowercase name -> { worldPos, worldQuat, node }

        model.updateWorldMatrix(true, true);
        model.traverse((node) => {
            const name = (node.name || '').toLowerCase().trim();
            if (!name) return;
            const worldPos = new THREE.Vector3();
            const worldQuat = new THREE.Quaternion();
            node.getWorldPosition(worldPos);
            node.getWorldQuaternion(worldQuat);
            this._fbxNodes.set(name, { worldPos, worldQuat, node });
        });

        console.log(`[VRayConverter] Extracted ${this._fbxNodes.size} FBX nodes for transform lookup`);

        // Debug: log which cameras/lights/targets we found
        for (const cam of this.cameras) {
            const n = (cam.name || '').toLowerCase();
            const found = this._fbxNodes.has(n);
            console.log(`[VRayConverter]   Camera "${cam.name}" -> FBX node: ${found ? 'FOUND' : 'NOT FOUND'}`);
        }
        for (const ld of this.lights) {
            const n = (ld.name || '').toLowerCase();
            const found = this._fbxNodes.has(n);
            console.log(`[VRayConverter]   Light "${ld.name}" (${ld.type}) -> FBX node: ${found ? 'FOUND' : 'NOT FOUND'}`);
            // Also check for target
            const tIdx = ld.params.targetidx;
            if (tIdx !== undefined) {
                const tObj = this.rawObjs.find(o => o.idx === tIdx);
                if (tObj) {
                    const tn = (tObj.ocname || '').toLowerCase();
                    const tFound = this._fbxNodes.has(tn);
                    console.log(`[VRayConverter]     Target "${tObj.ocname}" -> FBX node: ${tFound ? 'FOUND' : 'NOT FOUND'}`);
                }
            }
        }
    }

    /** Get world position from FBX node by name, with fallback to JSON position.
     *  If FBX space is raw Z-up, JSON pos is used as-is; otherwise converted. */
    _getPos(name, jsonPos) {
        const n = (name || '').toLowerCase();
        if (this._fbxNodes?.has(n)) {
            return this._fbxNodes.get(n).worldPos.clone();
        }
        // Fallback: use JSON position in the same space as FBX
        if (Array.isArray(jsonPos) && jsonPos.length >= 3) {
            return this._fbxIsRawZUp
                ? new this.THREE.Vector3(jsonPos[0], jsonPos[1], jsonPos[2])
                : this._zUpToYUp(jsonPos[0], jsonPos[1], jsonPos[2]);
        }
        return new this.THREE.Vector3(0, 0, 0);
    }

    _zUpToYUp(x, y, z) {
        return new this.THREE.Vector3(x, z, -y);
    }

    /* ─── Lights (max 1 dir + 3 point, no point shadows) ─── */

    applyLights(scene) {
        if (!this.lights.length) return;
        const THREE = this.THREE;

        // Remove existing dir/point/spot
        const rm = [];
        scene.traverse(c => { if (c.isDirectionalLight || c.isPointLight || c.isSpotLight) rm.push(c); });
        rm.forEach(l => l.parent?.remove(l));

        // Separate sun(s) and area/point lights
        const suns = this.lights.filter(l => l.type === 'VRaySun');
        const others = this.lights.filter(l => l.type === 'VRayLight');

        // Add 1 sun (the first enabled, or just first)
        const sunDef = suns.find(s => s.params.enabled) || suns[0];
        if (sunDef) {
            const p = sunDef.params;
            const mul = parseFloat(p.intensity_multiplier || 1);
            const light = new THREE.DirectionalLight(0xffffff, Math.min(mul * 1.5, 5));

            // Use FBX node position (already in correct Three.js space)
            light.position.copy(this._getPos(sunDef.name, p.position || p.nodepos));

            // Sun target: look up by target object name in FBX
            const ti = p.targetidx;
            if (ti !== undefined) {
                const tObj = this.rawObjs.find(o => o.idx === ti);
                if (tObj) {
                    light.target.position.copy(this._getPos(tObj.ocname, tObj.position || tObj.nodepos));
                    scene.add(light.target);
                }
            }

            light.castShadow = true;
            light.shadow.mapSize.set(1024, 1024);
            light.shadow.camera.near = 0.5;
            light.shadow.camera.far = 500;
            const S = 50;
            light.shadow.camera.left = -S;
            light.shadow.camera.right = S;
            light.shadow.camera.top = S;
            light.shadow.camera.bottom = -S;
            scene.add(light);
        }

        // Add up to 3 most important point lights (highest multiplier), NO shadows
        const sorted = others
            .filter(l => l.params.on !== false)
            .sort((a, b) => Math.abs(parseFloat(b.params.multiplier || 0)) - Math.abs(parseFloat(a.params.multiplier || 0)));

        const MAX_POINTS = 3;
        const added = sorted.slice(0, MAX_POINTS);
        const maxMul = added.reduce((m, l) => Math.max(m, Math.abs(parseFloat(l.params.multiplier || 1))), 1);

        for (const ld of added) {
            const p = ld.params;
            const mul = Math.abs(parseFloat(p.multiplier || 1));
            const intensity = 0.3 + (mul / maxMul) * 2.7;

            const cArr = p.color || [255, 255, 255, 255];
            const d = (cArr[3] > 1) ? cArr[3] : 255;
            const color = new THREE.Color(cArr[0] / d, cArr[1] / d, cArr[2] / d);

            const light = new THREE.PointLight(color, intensity, 0, 2);
            // Use FBX node position
            light.position.copy(this._getPos(ld.name, p.position || p.nodepos));
            light.castShadow = false;
            scene.add(light);
        }

        // Ambient fill
        scene.add(new THREE.AmbientLight(0xffffff, 0.3));

        console.log(`[VRayConverter] Lights: ${sunDef ? 1 : 0} sun + ${added.length} points (of ${this.lights.length} total, FBX nodes: ${this._fbxNodes ? 'YES' : 'NO'})`);
    }

    /* ─── Cameras ─── */

    /**
     * Detect whether FBXLoader preserved original Z-up coordinates (no conversion)
     * or applied Y-up conversion.  Compares FBX node world positions with JSON
     * positions to determine the coordinate space empirically.
     *
     * Also determines which FBX matrix column corresponds to the camera's
     * forward (look) direction by comparing all 6 axes against the JSON `dir`.
     */
    _detectAxisMapping() {
        if (this._axisDetected !== undefined) return;
        this._axisDetected = true;
        this._fbxIsRawZUp = false;        // if true, FBX kept original Z-up coords
        this._directionFromMatrix = false; // if true, use FBX matrix column for direction
        this._bestForwardAxis = null;      // which matrix column is camera "forward"

        const THREE = this.THREE;
        for (const camDef of this.cameras) {
            const p = camDef.params;
            const name = camDef.name;
            const fbxEntry = this._fbxNodes?.get(name.toLowerCase());
            if (!fbxEntry) continue;

            const jsonPos = p.position || p.nodepos;
            if (!Array.isArray(jsonPos) || jsonPos.length < 3) continue;

            const fbxPos = fbxEntry.worldPos;
            const rawJsonPos = new THREE.Vector3(jsonPos[0], jsonPos[1], jsonPos[2]);
            const convJsonPos = this._zUpToYUp(jsonPos[0], jsonPos[1], jsonPos[2]);

            const distRaw = fbxPos.distanceTo(rawJsonPos);
            const distConv = fbxPos.distanceTo(convJsonPos);

            console.log(`[VRayConverter] Axis detect — Camera "${name}":`);
            console.log(`  FBX worldPos:      ${fbxPos.x.toFixed(3)}, ${fbxPos.y.toFixed(3)}, ${fbxPos.z.toFixed(3)}`);
            console.log(`  JSON raw:          ${rawJsonPos.x.toFixed(3)}, ${rawJsonPos.y.toFixed(3)}, ${rawJsonPos.z.toFixed(3)}  dist=${distRaw.toFixed(4)}`);
            console.log(`  JSON _zUpToYUp:    ${convJsonPos.x.toFixed(3)}, ${convJsonPos.y.toFixed(3)}, ${convJsonPos.z.toFixed(3)}  dist=${distConv.toFixed(4)}`);

            // If raw JSON pos matches FBX pos, FBXLoader kept Z-up coordinates
            this._fbxIsRawZUp = (distRaw < distConv) && (distRaw < 0.1);
            console.log(`  FBX coord space: ${this._fbxIsRawZUp ? 'Z-UP (raw, no conversion)' : 'Y-UP (converted)'}`);

            // Determine which matrix column = camera forward direction
            const dir = p.dir;
            if (!Array.isArray(dir) || dir.length < 3) continue;

            // JSON dir as raw Vector3 (same space as FBX if _fbxIsRawZUp)
            const jsonDirRaw = new THREE.Vector3(dir[0], dir[1], dir[2]).normalize();

            const e = fbxEntry.node.matrixWorld.elements;
            const axes = [
                { name: '+X', v: new THREE.Vector3(e[0], e[1], e[2]).normalize() },
                { name: '-X', v: new THREE.Vector3(-e[0], -e[1], -e[2]).normalize() },
                { name: '+Y', v: new THREE.Vector3(e[4], e[5], e[6]).normalize() },
                { name: '-Y', v: new THREE.Vector3(-e[4], -e[5], -e[6]).normalize() },
                { name: '+Z', v: new THREE.Vector3(e[8], e[9], e[10]).normalize() },
                { name: '-Z', v: new THREE.Vector3(-e[8], -e[9], -e[10]).normalize() },
            ];

            let bestAxis = null, bestDot = -2;
            for (const ax of axes) {
                const dot = ax.v.dot(jsonDirRaw);
                const angle = Math.acos(Math.min(1, Math.max(-1, dot))) * 180 / Math.PI;
                console.log(`    Matrix ${ax.name}: (${ax.v.x.toFixed(4)}, ${ax.v.y.toFixed(4)}, ${ax.v.z.toFixed(4)}) dot=${dot.toFixed(4)} angle=${angle.toFixed(1)}°`);
                if (dot > bestDot) { bestDot = dot; bestAxis = ax; }
            }
            console.log(`  BEST forward axis: ${bestAxis.name} (dot=${bestDot.toFixed(4)})`);

            if (bestDot > 0.95) {
                this._directionFromMatrix = true;
                this._bestForwardAxis = bestAxis.name;
                console.log(`  >>> Will use FBX matrix ${bestAxis.name} column as camera forward`);
            } else {
                console.log(`  >>> Using JSON dir as-is (FBX space matches JSON space)`);
            }
            break;
        }
    }

    /** Extract forward direction from FBX node's world matrix using detected axis */
    _getFbxForward(fbxEntry) {
        const e = fbxEntry.node.matrixWorld.elements;
        const THREE = this.THREE;
        switch (this._bestForwardAxis) {
            case '+X': return new THREE.Vector3(e[0], e[1], e[2]).normalize();
            case '-X': return new THREE.Vector3(-e[0], -e[1], -e[2]).normalize();
            case '+Y': return new THREE.Vector3(e[4], e[5], e[6]).normalize();
            case '-Y': return new THREE.Vector3(-e[4], -e[5], -e[6]).normalize();
            case '+Z': return new THREE.Vector3(e[8], e[9], e[10]).normalize();
            case '-Z': return new THREE.Vector3(-e[8], -e[9], -e[10]).normalize();
            default:   return null;
        }
    }

    getCameras() {
        const THREE = this.THREE;
        this._detectAxisMapping();

        return this.cameras.map(camDef => {
            const p = camDef.params;
            const name = camDef.name;

            // Position: from FBX node (already in correct scene space)
            const threePos = this._getPos(name, p.position || p.nodepos);
            const fov = parseFloat(p.fov) || 45;
            const dist = parseFloat(p.target_distance) || 10;
            let target = null;
            let dirSource = 'none';

            // Priority 1: FBX target-node (e.g. "vraycam001.Target")
            const targetNames = [name + '.target', name + '_target', name + ' target'];
            for (const tn of targetNames) {
                if (this._fbxNodes?.has(tn.toLowerCase())) {
                    target = this._fbxNodes.get(tn.toLowerCase()).worldPos.clone();
                    dirSource = 'FBX target node';
                    break;
                }
            }

            // Priority 2: FBX matrix best-axis (if auto-detection found a good match)
            if (!target && this._directionFromMatrix && this._bestForwardAxis) {
                const fbxEntry = this._fbxNodes?.get(name.toLowerCase());
                if (fbxEntry) {
                    const forward = this._getFbxForward(fbxEntry);
                    if (forward) {
                        target = threePos.clone().add(forward.multiplyScalar(dist));
                        dirSource = `FBX matrix ${this._bestForwardAxis}`;
                    }
                }
            }

            // Priority 3: JSON dir — use RAW if FBX is Z-up, convert if FBX is Y-up
            if (!target && Array.isArray(p.dir) && p.dir.length >= 3) {
                const td = this._fbxIsRawZUp
                    ? new THREE.Vector3(p.dir[0], p.dir[1], p.dir[2]).normalize()
                    : this._zUpToYUp(p.dir[0], p.dir[1], p.dir[2]).normalize();
                target = threePos.clone().add(td.multiplyScalar(dist));
                dirSource = this._fbxIsRawZUp ? 'JSON dir (raw Z-up)' : 'JSON dir (_zUpToYUp)';
            }

            console.log(`[VRayConverter] Camera "${name}": pos=(${threePos.x.toFixed(2)}, ${threePos.y.toFixed(2)}, ${threePos.z.toFixed(2)})` +
                (target ? ` target=(${target.x.toFixed(2)}, ${target.y.toFixed(2)}, ${target.z.toFixed(2)})` : '') +
                ` fov=${fov.toFixed(1)} dir=${dirSource}`);

            return { name, position: threePos, fov, target };
        });
    }
}
