from typing import Any, Dict
import pytest
import json
from pathlib import Path
import typer
from app.manage_db.utils import parse_collection_release_input_json

@pytest.fixture
def temp_json_file(tmp_path:Path):
    """Creates a temporary JSON file with valid input data."""
    json_data: Dict[str, Any] = {
        "collection": {
            "name": "Test dataset",
            "collection_description": "Test dataset is a collection of pangenome made of 3 species with very small genomes."
        },
        "release": {
            "version": "0.0.1",
            "ppanggolin_version": "2.1.0",
            "pangbank_wf_version": "0.0.1",
            "pangenomes_directory": "pangenomes",
            "release_note": "This release is just a test release.",
            "date": "2025-01-29",
            "mash_sketch": "mash_sketch/families_persistent_all.msh",
            "mash_version": "2.3"
        },
        "taxonomy": {
            "name": "GTDB",
            "version": "24.1",
            "ranks": "Domain; Phylum; Class; Order; Family; Genus; Species",
            "file": "taxonomy.tsv.gz"
        },
        "genome_sources": [
            {
                "name": "RefSeq",
                "file": "RefSeq.list",
                "version": "",
                "description": "",
                "source": "",
                "url": ""
            }
        ]
    }

    json_file = tmp_path / "input.json"
    pangenome_dir = tmp_path / "pangenomes"
    genome_source_file = tmp_path / "RefSeq.list"
    taxonomy_file =   tmp_path / "taxonomy.tsv.gz"

    pangenome_dir.mkdir(parents=True, exist_ok=True)
    genome_source_file.touch()
    taxonomy_file.touch()

    json_file.write_text(json.dumps(json_data))
    return json_file


def test_parse_collection_release_input_json(temp_json_file:Path):
    """Test that the function correctly parses and validates the input JSON."""
    data = parse_collection_release_input_json(temp_json_file)

    # Validate key data points
    assert data.release.version == "0.0.1"
    assert data.taxonomy.name == "GTDB"
    assert len(data.genome_sources) == 1
    assert data.genome_sources[0].file == temp_json_file.parent / "RefSeq.list"
    assert data.release.pangenomes_directory == str(temp_json_file.parent / "pangenomes")


def test_parse_collection_release_input_json_no_input(tmp_path:Path):
    """Test that the function correctly parses and validates the input JSON."""
    no_existing_path = tmp_path / "no_existing.json"
    with pytest.raises(typer.Exit):
        parse_collection_release_input_json(no_existing_path)

def test_parse_collection_release_input_json_invalid_json(tmp_path:Path):
    """Test that the function correctly parses and validates the input JSON."""
    invalid_json_path = tmp_path / "invalid.json"
    invalid_json_path.write_text("this is not a json")
    
    with pytest.raises(typer.Exit):
        parse_collection_release_input_json(invalid_json_path)
