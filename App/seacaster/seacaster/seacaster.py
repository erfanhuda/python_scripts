from typing import Any, Dict, NamedTuple


class CurrentToDo(NamedTuple):
    todo: Dict[str, Any]
    error: int