import json
import logging
from pathlib import Path

import typer
from rich.logging import RichHandler

from app.models import CollectionReleaseInput


def check_and_read_json_file(input_json_file: Path):

    if not input_json_file.exists():
        typer.echo(
            f"[bold red]Error:[/bold red] JSON file '{input_json_file}' does not exist.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        with open(input_json_file) as fl:
            json_content = json.load(fl)

    except json.JSONDecodeError as e:
        typer.echo(f"[bold red]Error:[/bold red] Invalid JSON format: {e}", err=True)
        raise typer.Exit(1)

    return json_content


def parse_collection_release_input_json(collection_release_json: Path):

    json_content = check_and_read_json_file(collection_release_json)
    # Validate JSON structure using Pydantic
    data_input = CollectionReleaseInput.model_validate(json_content)

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


def set_up_logging_config():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[RichHandler()],
    )
