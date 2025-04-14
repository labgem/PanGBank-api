import json
import logging
from pathlib import Path
import hashlib
from sqlmodel import SQLModel  # type: ignore


import typer
from rich.logging import RichHandler

from pangbank_api.manage_db.input_models import CollectionReleaseInput


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


def parse_collection_release_input_json(
    collection_release_json: Path, pangbank_data_dir: Path
):
    json_content = check_and_read_json_file(collection_release_json)
    mash_sketch_md5sum = compute_md5(
        pangbank_data_dir / json_content["release"]["mash_sketch"]
    )
    json_content["release"]["mash_sketch_md5sum"] = mash_sketch_md5sum
    # Validate JSON structure using Pydantic
    data_input = CollectionReleaseInput.model_validate(json_content)

    data_input.taxonomy.file = pangbank_data_dir / data_input.taxonomy.file

    for genome_source in data_input.genome_sources:
        genome_source.file = pangbank_data_dir / genome_source.file

    pangenomes_directory = pangbank_data_dir / data_input.release.pangenomes_directory
    mash_sketch_file = pangbank_data_dir / data_input.release.mash_sketch

    # Check if paths exist
    missing_files = [
        data_input.taxonomy.file,
        pangenomes_directory,
        mash_sketch_file,
    ] + [gs.file for gs in data_input.genome_sources]
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


def compute_md5(file_path: Path):
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):  # Read file in chunks
            md5_hash.update(chunk)
    return md5_hash.hexdigest()
