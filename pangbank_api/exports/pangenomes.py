from collections.abc import Callable

from pangbank_api.models import Pangenome


COLLECTION_EXPORT_COLUMNS: tuple[tuple[str, Callable[[Pangenome], str]], ...] = (
    ("Pangenome_id", lambda p: str(p.id)),
    ("Pangenome_name", lambda p: p.name),
    ("Taxonomy", lambda p: ";".join(p.taxonomy.keys())),  # type: ignore
    ("Merged_species", lambda p: str(p.has_multiple_species)),
    ("Genomes", lambda p: str(p.genome_count)),
    ("Isolate", lambda p: str(p.genome_category_counts.get("Isolate", 0))), # type: ignore
    ("MAGs", lambda p: str(p.genome_category_counts.get("MAGs", 0))), # type: ignore
    ("SAGs", lambda p: str(p.genome_category_counts.get("SAGs", 0))), # type: ignore
    ("Unknown", lambda p: str(p.genome_category_counts.get("Unknown", 0))), # type: ignore
    ("Genes", lambda p: str(p.gene_count)),
    ("Families", lambda p: str(p.family_count)),
    ("Persistent", lambda p: str(p.persistent_family_count)),
    ("Shell", lambda p: str(p.shell_family_count)),
    ("Cloud", lambda p: str(p.cloud_family_count)),
    ("Partitions", lambda p: str(p.partition_count)),
    ("Spots", lambda p: str(p.spot_count)),
    ("RGPs", lambda p: str(p.rgp_count)),
    ("Modules", lambda p: str(p.module_count)),
    ("Average_families", lambda p: f"{p.average_families_per_genome:.1f}"), # type: ignore
    ("Persistent_fraction", lambda p: f"{p.persistent_fraction * 100:.1f}"), # type: ignore
    ("Shell_fraction", lambda p: f"{p.shell_fraction * 100:.1f}"), # type: ignore
    ("Cloud_fraction", lambda p: f"{p.cloud_fraction * 100:.1f}"), # type: ignore
)


def build_table_of_pangenomes(
    pangenomes: list[Pangenome],
) -> tuple[list[str], list[list[str]]]:
    """
    Build a tabular representation of pangenomes.

    Returns:
        headers: column names
        rows: formatted values
    """

    headers = [
        header
        for header, _formatter in COLLECTION_EXPORT_COLUMNS
    ]

    rows = [
        [
            formatter(pangenome)
            for _header, formatter in COLLECTION_EXPORT_COLUMNS
        ]
        for pangenome in pangenomes
    ]

    return headers, rows


def build_tsv_of_pangenomes(
    pangenomes: list[Pangenome],
) -> str:
    """
    Convert a list of pangenomes to a TSV string.

    Returns:
        A string representing the pangenomes in TSV format.
    """
    headers, rows = build_table_of_pangenomes(pangenomes)

    lines = [
        "\t".join(headers),
        *[
            "\t".join(row)
            for row in rows
        ],
    ]

    return "\n".join(lines) + "\n"
