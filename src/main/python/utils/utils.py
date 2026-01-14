from pathlib import Path


__all__ = [
    "get_project_root"
]


def get_project_root(root_name: str = "DE_Learning", fpath: str | Path = None) -> Path:
    """
    Retrieves the root directory of the project.

    Returns:
        Path: The root directory of the project
    """
    if type(fpath) is str:
        fpath = Path(fpath)

    while fpath.name != root_name:
        fpath = fpath.parent

    return fpath
