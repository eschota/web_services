"""Real integration test with renderfin.com API."""

import asyncio
import json
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


async def test_real_render():
    """Test real render with status tracking via JSON metadata."""
    output_path = "R:/work/renderfin-mcp/vars/pics/test_horse.png"
    json_path = Path(f"{output_path}.json")
    
    # Clean up any previous test files
    if json_path.exists():
        json_path.unlink()
    if Path(output_path).exists():
        Path(output_path).unlink()
    
    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Output path: {output_path}")
    print(f"JSON path: {json_path}")
    print("-" * 50)
    
    # 1. Call schedule_render (via direct import)
    from renderfin_mcp.server import schedule_render
    
    result = await schedule_render(
        prompt="A majestic horse dancing gracefully in a golden field at sunset",
        absolute_output_path=output_path,
        aspect_ratio=1.0
    )
    
    print(f"schedule_render result: {result}")
    assert result["status"] == "scheduled", f"Expected 'scheduled', got: {result['status']}"
    print("-" * 50)
    
    # 2. Wait for JSON to appear and track status changes
    statuses = []
    timeout = 360  # 6 minutes max (rendering can take 2-5 min)
    start = time.time()
    
    while time.time() - start < timeout:
        if json_path.exists():
            try:
                data = json.loads(json_path.read_text(encoding="utf-8"))
                current_status = data.get("status")
                
                if current_status and current_status not in statuses:
                    statuses.append(current_status)
                    elapsed = time.time() - start
                    print(f"[{elapsed:.1f}s] Status: {current_status}")
                    
                    # Print additional info
                    if current_status == "downloading" and "output_url" in data:
                        print(f"         output_url: {data['output_url']}")
                    if current_status == "completed" and "file_size" in data:
                        print(f"         file_size: {data['file_size']} bytes")
                
                if current_status == "completed":
                    # Verify image exists
                    assert Path(output_path).exists(), "Image file not found!"
                    file_size = Path(output_path).stat().st_size
                    print(f"\nImage saved! Size: {file_size} bytes")
                    break
                    
                elif current_status == "failed":
                    error = data.get("error", "Unknown error")
                    raise Exception(f"Task failed: {error}")
                    
            except json.JSONDecodeError:
                pass  # File might be mid-write
        
        await asyncio.sleep(1)
    else:
        raise TimeoutError(f"Test timed out after {timeout} seconds")
    
    # 3. Verify we saw status transitions
    print("-" * 50)
    print(f"Status transitions: {' -> '.join(statuses)}")
    
    assert "queued" in statuses, "Missing 'queued' status"
    # Note: 'processing' may be too fast to catch, 'downloading' is the important middle state
    assert "downloading" in statuses or "processing" in statuses, "Missing 'downloading' or 'processing' status"
    assert "completed" in statuses, "Missing 'completed' status"
    
    # 4. Print final JSON
    print("-" * 50)
    print("Final metadata JSON:")
    print(json.dumps(json.loads(json_path.read_text(encoding="utf-8")), indent=2))
    
    print("\n[OK] Test passed!")


if __name__ == "__main__":
    asyncio.run(test_real_render())
