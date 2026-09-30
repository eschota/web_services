# Level 1 — "До острова" (the jet)

Extracted from the concept chat of 2026-09-30. The prototype files in this
folder (`dist/`, `README.md`, tests) are the ChatGPT export
`jet-first-level-source.zip`, unchanged.

## Setting

Private business jet, owner's cabin: bed, glass shower with a marble finish,
portholes over a tropical sea. **Interior style chosen: dark futurism** —
graphite, carbon, brushed steel, strip lighting. Cabin size from the 3D
block-out: 3.65 × 4.4 × 2.4 m (calibrated by a 2 m bed and the depth map).

## Flow

1. **Wake-up.** Two knocks, light through the porthole shade, the phone buzzes:
   "We'll talk when you land. Dad." From behind the door: "We're starting our
   descent soon. Are you alive in there?"
   Choices: joke about coffee · "One minute" (dress, look around) · open the
   door silently · pretend to sleep.
2. **Explore the cabin:** phone, clothes, porthole, the father's envelope.
3. **Three meetings with Sura** (working name Mira): at the door, in the salon,
   before landing — free exploration between them.
4. **Landing:** wheels touch down, the door opens, the father waits below with
   a woman who calls the hero by name.

## Chains (combine freely)

| # | Chain | Result |
|---|---|---|
| 1 | "Surprise me" — mutual interest; she wants the cockpit; he admits he does not know how to meet his father's partner | She leaves a contact: "Only I choose the place" |
| 2 | "The famous stranger" — he lies about who he is; she knew his name all along | Laugh it off / open up / get cold; on the island she uses the fake name |
| 3 | "Can you just listen?" — friendship; interview rehearsal; she rehearses his meeting with the partner | An ally on the island |
| 4 | "Service included" — commands, interrupts, tries a gift | Strictly professional; an apology fixes the tone, not the outcome |
| 5 | "Why did we change course?" — no flirting; the envelope and the long-stay luggage | Family mystery opens early |

Recommended core for the demo: 1 + 2 + 5. Six endings in the prototype:
contact, support, awkward acquaintance, neutral landing, the father's secret,
a non-graphic game over.

## Assets

- Concept art: `dist/assets/C01–C06` (cabin), `S01–S06` (shower), `wake`.
- Depth graph for the chosen interior: https://autorig.online/nodes?g=7f85772d24f8
  (Media In → ControlNet Depth, 1672×941).
- Blender block-out with 6 + 12 cameras and an animated walk-through, and a
  Three.js viewer — made in the ChatGPT session; **not in this repo yet**
  (the archives are in the owner's ChatGPT library).

## Canon change

The flight attendant is **Sura**, a secret agent (owner, 2026-09-30). Rename
"Mira" in `dist/story.json` when the script is revised; her cover and dialogue
stay, with hints of the agent added.
