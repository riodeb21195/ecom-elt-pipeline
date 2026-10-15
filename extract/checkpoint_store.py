import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

CHECKPOINT_FILE = Path(__file__).parent / ".checkpoints.json"


@dataclass
class RunMetadata:
    source: str
    started_at: str
    finished_at: Optional[str]
    rows_extracted: int
    status: str
    error: Optional[str] = None


def get_last_checkpoint(source: str) -> Optional[str]:
    if not CHECKPOINT_FILE.exists():
        return None
    data = json.loads(CHECKPOINT_FILE.read_text())
    return data.get(source, {}).get("last_updated_at")
    
def save_checkpoint(source: str, last_updated_at: str, run_metadata: RunMetadata) -> None:
    data = {}
    if CHECKPOINT_FILE.exists():
        data = json.loads(CHECKPOINT_FILE.read_text())

    data[source] = {
        "last_updated_at": last_updated_at,
        "last_run": asdict(run_metadata),
        "run_history": data.get(source, {}).get("run_history", [])[-9:] + [asdict(run_metadata)],
    }
    CHECKPOINT_FILE.write_text(json.dumps(data, indent=2))
