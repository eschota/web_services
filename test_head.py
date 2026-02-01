import httpx
import asyncio

async def test():
    worker_base = "http://5.129.157.224:5132"
    guid = "8e59f1f5-2404-478e-a888-6d8a2f72fca7"
    animations_url = f"{worker_base}/converter/glb/{guid}/{guid}_all_animations.glb"
    
    async with httpx.AsyncClient() as client:
        try:
            head_resp = await client.head(animations_url, timeout=10.0, follow_redirects=True)
            print(f"HEAD {animations_url} -> {head_resp.status_code}")
        except Exception as e:
            print(f"HEAD {animations_url} -> Error: {e}")

asyncio.run(test())
