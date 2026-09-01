import assert from 'node:assert/strict';
import test from 'node:test';

import {
    assessMeaningfulNonRootLocalMotion,
    evaluateSemanticVisualQa,
} from '../browser_animation_visual_phase_qa.mjs';

const TIMES = [0, 1 / 30, 2 / 30];

function track(name, values) {
    return { name, times: TIMES, values };
}

function airborneResult(groundContact) {
    return evaluateSemanticVisualQa({
        policy: { generationMode: 'one_shot', terminalPolicy: 'airborne_transition' },
        deformationReport: {
            schema: 'autorig.browser-horse-target-deformation-qa.v1',
            passed: true,
        },
        finalPoseReport: {
            schema: 'autorig.browser-horse-one-shot-final-pose-qa.v1',
            gates: {
                groundContact,
                groundPenetration: true,
                cameraStatic: true,
            },
        },
    });
}

test('jump airborne transition requires positive no-ground-contact evidence', () => {
    const airborne = airborneResult(false);
    assert.equal(airborne.machinePassed, true);
    assert.deepEqual(airborne.terminalGates, {
        airborne: true,
        groundPenetration: true,
        cameraStatic: true,
    });

    const grounded = airborneResult(true);
    assert.equal(grounded.machinePassed, false);
    assert.equal(grounded.terminalGates.airborne, false);

    const missing = airborneResult(undefined);
    assert.equal(missing.machinePassed, false);
    assert.equal(missing.terminalGates.airborne, false);
});

test('root travel plus epsilon limb noise is not meaningful skeletal motion', () => {
    const result = assessMeaningfulNonRootLocalMotion({
        frameCount: TIMES.length,
        rootBoneNames: new Set(['Root']),
        tracks: [
            track('Root.position', [0, 0, 0, 1, 0, 0, 2, 0, 0]),
            track('Limb.position', [0, 0, 0, 0.000001, 0, 0, 0.000002, 0, 0]),
            track('Limb.quaternion', [
                0, 0, 0, 1,
                0, 0, 0.000001, 1,
                0, 0, 0.000002, 1,
            ]),
        ],
    });

    assert.equal(result.passed, false);
    assert.deepEqual(result.movingBoneNames, []);
    assert.ok(result.maximumPositionDeltaWorld < result.thresholds.minimumPositionDeltaWorld);
    assert.ok(result.maximumQuaternionAngleRad < result.thresholds.minimumQuaternionAngleRad);
});

test('meaningful local limb rotation passes independently of root travel', () => {
    const halfAngle = 0.1;
    const result = assessMeaningfulNonRootLocalMotion({
        frameCount: TIMES.length,
        rootBoneNames: new Set(['Root']),
        tracks: [
            track('Root.position', [0, 0, 0, 1, 0, 0, 2, 0, 0]),
            track('Limb.position', [0, 0, 0, 0, 0, 0, 0, 0, 0]),
            track('Limb.quaternion', [
                0, 0, 0, 1,
                0, 0, Math.sin(halfAngle / 2), Math.cos(halfAngle / 2),
                0, 0, Math.sin(halfAngle), Math.cos(halfAngle),
            ]),
        ],
    });

    assert.equal(result.passed, true);
    assert.deepEqual(result.movingBoneNames, ['Limb']);
    assert.ok(result.maximumQuaternionAngleRad >= 0.2 - 1e-12);
});
