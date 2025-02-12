from pathlib import Path
import json
from app.models import CollectionReleaseInput

import typer

from pydantic import ValidationError


def check_collection_release_input_json(collection_release_json: Path):

    if not collection_release_json.exists():
        typer.echo(
            f"[bold red]Error:[/bold red] JSON file '{collection_release_json}' does not exist.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        with open(collection_release_json) as fl:
            all_info = json.load(fl)

        # Validate JSON structure using Pydantic
        data_input = CollectionReleaseInput.model_validate(all_info)

    except json.JSONDecodeError as e:
        typer.echo(f"[bold red]Error:[/bold red] Invalid JSON format: {e}", err=True)
        raise typer.Exit(1)
    except ValidationError as e:
        typer.echo(
            f"[bold red]Error:[/bold red] JSON structure validation failed:\n{e}",
            err=True,
        )
        raise typer.Exit(1)

    data_input.taxonomy.file = collection_release_json.parent / data_input.taxonomy.file

    for genome_source in data_input.genome_sources:
        genome_source.file = collection_release_json.parent / genome_source.file

    pangenomes_directory = (
        collection_release_json.parent / data_input.release.pangenomes_directory
    )
    data_input.release.pangenomes_directory = str(pangenomes_directory)

    # Check if paths exist
    missing_files = [data_input.taxonomy.file, pangenomes_directory] + [
        gs.file for gs in data_input.genome_sources
    ]
    missing_files = [f for f in missing_files if not f.exists()]

    if missing_files:
        typer.echo(
            "[bold red]Error:[/bold red] The following files are missing:", err=True
        )
        for f in missing_files:
            typer.echo(f"  - {f}", err=True)
        raise typer.Exit(1)

    return data_input
