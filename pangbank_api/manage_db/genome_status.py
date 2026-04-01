import gzip
import logging
from pathlib import Path
from typing import Generator

from rich.progress import track
from sqlmodel import Session, select

from pangbank_api.manage_db.input_models import GenomeStatusInput
from pangbank_api.models import CollectionRelease, Genome, GenomeStatus

logger = logging.getLogger(__name__)


def parse_genome_status_file(
    file_path: Path,
    disable_track: bool = False,
) -> Generator[str, None, None]:
    """
    Parse a text file containing genome names (one per line).
    Supports both plain text and gzip-compressed files.
    
    Args:
        file_path: Path to the text file (can be .txt or .txt.gz)
        disable_track: Whether to disable progress tracking
        
    Yields:
        Genome names
    """
    proper_open = gzip.open if file_path.name.endswith("gz") else open
    with proper_open(file_path, "rt") as f:
        lines = [line.strip() for line in f if line.strip()]
    
    for genome_name in track(lines, f"Parsing {file_path.name}", disable=disable_track):
        yield genome_name


def add_genome_statuses_to_release(
    collection_release: CollectionRelease,
    genome_status_inputs: list[GenomeStatusInput],
    genome_name_to_genome: dict[str, Genome],
    session: Session,
) -> None:
    """
    Add genome status information (representative/reference) to genomes in a release.
    
    Args:
        collection_release: The collection release to add statuses to
        genome_status_inputs: List of genome status input configurations
        genome_name_to_genome: Mapping of genome names to Genome objects
        session: Database session
    """
    if not genome_status_inputs:
        logger.info("No genome status inputs provided, skipping.")
        return
    
    for status_input in genome_status_inputs:
        logger.info(
            f"Processing genome statuses: status_type={status_input.status_type}, "
            f"origin={status_input.origin}, file={status_input.file}"
        )
        
        genome_names = list(parse_genome_status_file(status_input.file))
        
        found_count = 0
        missing_count = 0
        skipped_count = 0
        genome_statuses : list[GenomeStatus] = []
        
        for genome_name in genome_names:
            genome = genome_name_to_genome.get(genome_name)
            
            if genome is None:
                missing_count += 1
                continue
            
            if genome.id is None:
                logger.warning(
                    f"Genome {genome_name} does not have an ID. Skipping status assignment."
                )
                continue
            
            if collection_release.id is None:
                logger.error("Collection release does not have an ID. Cannot add genome statuses.")
                continue
            
            # Check if this genome status already exists
            existing_status = session.exec(
                select(GenomeStatus).where(
                    GenomeStatus.genome_id == genome.id,
                    GenomeStatus.collection_release_id == collection_release.id,
                    GenomeStatus.status_type == status_input.status_type,
                    GenomeStatus.origin == status_input.origin,
                )
            ).first()
            
            if existing_status:
                skipped_count += 1
                logger.debug(
                    f"Genome status already exists for {genome_name} "
                    f"(status_type={status_input.status_type}, origin={status_input.origin}). Skipping."
                )
                continue
            
            # Create GenomeStatus entry
            genome_status = GenomeStatus(
                genome_id=genome.id,
                collection_release_id=collection_release.id,
                status_type=status_input.status_type,
                origin=status_input.origin,
            )
            genome_statuses.append(genome_status)
            found_count += 1
        
        # Add all statuses to the database
        if genome_statuses:
            session.add_all(genome_statuses)
            logger.info(
                f"Added {found_count} genome statuses "
                f"(status_type={status_input.status_type}, origin={status_input.origin})"
            )
        
        if skipped_count > 0:
            logger.info(
                f"Skipped {skipped_count} genome statuses that already exist "
                f"for status_type={status_input.status_type}, origin={status_input.origin}."
            )
        
        if missing_count > 0:
            logger.info(
                f"Skipped {missing_count} genomes not found in the database "
                f"for status_type={status_input.status_type}, origin={status_input.origin}."
            )
    
    session.commit()
    logger.info("Genome status processing complete.")
