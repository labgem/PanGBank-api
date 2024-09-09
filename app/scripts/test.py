from sqlmodel import Session, select
from pathlib import Path
import json
import csv
import sys

# Add the project root to the sys.path
sys.path = [str(Path(__file__).resolve().parent.parent)] + sys.path

from app.database import create_db_and_tables, engine
from app.models import Collection, CollectionRelease, Genome, Pangenome, GenomePangenomeLink, GenomeSource

from taxonomy import create_taxonomy_source, parse_taxonomy_file, manage_genome_taxonomies, build_taxon_dict, parse_ranks_str
from app.crud import get_pangenome_file
    

def main():
    create_db_and_tables()

    
    with Session(engine) as session:

        pangenome_file = get_pangenome_file(session=session, pangenome_id=1)
        print(pangenome_file)

if __name__ == "__main__":
    main()