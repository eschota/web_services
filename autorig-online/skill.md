# AutoRig.online Agent Skill

AutoRig.online is a cloud service for automatic 3D model rigging and animation previews. Use the public site for human workflows and the agent API for automated upload, task tracking and download flows.

## Public Pages

- Home and upload: https://autorig.online/
- Gallery: https://autorig.online/gallery
- Animal and non-humanoid rigging: https://autorig.online/animal-rig
- Blender plugin: https://autorig.online/blender-plugin
- Developer overview: https://autorig.online/developers
- Buy credits: https://autorig.online/buy-credits

## Agent API

- Register an agent with `POST /api/agents/register`.
- Use the returned API key as a bearer token for authenticated agent requests.
- Upload supported 3D models through the task API, poll task status, then download generated outputs when complete.
- Respect server rate limits and retry only after transient failures.

## Supported Workflows

- Humanoid character auto-rigging.
- Animal and non-humanoid V2 rigging for models such as quadrupeds, creatures and spider robots.
- GLB, FBX and OBJ-oriented rigging flows.
- Browser workflow through AutoRig.online and Blender-native workflow through the separate Blender plugin.

## Discovery

- Sitemap index: https://autorig.online/sitemap.xml
- LLM discovery file: https://autorig.online/llm.txt
