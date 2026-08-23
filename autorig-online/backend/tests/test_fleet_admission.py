import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fleet_admission import fleet_admission_lock


def test_parallel_background_admissions_are_serialized_across_lock_handles():
    async def scenario(lock_path: Path):
        active = 0
        maximum = 0

        async def admission():
            nonlocal active, maximum
            async with fleet_admission_lock():
                active += 1
                maximum = max(maximum, active)
                await asyncio.sleep(0.05)
                active -= 1

        with patch.dict(
            "os.environ", {"AUTORIG_FLEET_ADMISSION_LOCK": str(lock_path)}
        ):
            await asyncio.gather(admission(), admission())
        return maximum

    with TemporaryDirectory() as directory:
        maximum = asyncio.run(scenario(Path(directory) / "fleet.lock"))
    assert maximum == 1
