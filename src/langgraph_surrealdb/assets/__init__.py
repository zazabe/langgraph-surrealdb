from pathlib import Path

import chevron

from langgraph_surrealdb import assets


def asset_path(relative: str) -> Path:
    """Return the filesystem path to a bundled asset.

    :param relative: Path relative to the assets package root.
    :returns: Resolved path to the asset file or directory.
    """
    try:
        return Path(assets.__file__).resolve().parent / relative
    except FileNotFoundError as exc:
        raise ValueError(f"Schema folder not found: {relative}") from exc


def render_schema(name: str, **kwargs: object) -> str:
    """Render a SurrealQL schema template with the given kwargs.
    Uses [[ and ]] as delimiters for the template.

    :param name: The name of the SurrealQL schema template.
    :param kwargs: The kwargs to render the SurrealQL schema template with.
    :returns: The rendered SurrealQL schema template.
    """
    with open(asset_path(f"{name}")) as f:
        template = f.read()
        return chevron.render(
            template=template,
            data=kwargs,
            def_ldel="[[",
            def_rdel="]]",
        )
