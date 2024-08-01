from datetime import date
from sqlmodel import Session, select
from pathlib import Path
import json

from .database import create_db_and_tables, engine
from .models import Collection, CollectionRelease, Genome, TaxonomySource, Pangenome
from datetime import datetime

from .taxonomy import create_genomes_and_taxonomies, create_taxonomy_source, parse_taxonomy_file


def associate_genomes_with_collection_release(genomes:list[Genome], collection_release:CollectionRelease, session:Session):

    for genome in genomes:
        if collection_release not in genome.collection_releases:
            genome.collection_releases.append(collection_release)

    session.refresh(collection_release)



def create_collection_release(collection_release_info_file:Path, session:Session) -> CollectionRelease:
    
    with open(collection_release_info_file) as fl:
        collection_release_info = json.load(fl)

    # Check if taxonomy release already exists in DB
    statement = (
        select(Collection)
        .where(
            (Collection.name == collection_release_info["name"]) 
        )
    )

    collection = session.exec(statement).first()

    if collection is None:
        print(f'Creating a new collection: {collection_release_info["name"]}')
        
        collection = Collection(name=collection_release_info["name"],
                                description=collection_release_info['description'])
        
        session.add(collection)


    else:
        print(f'Collection {collection.name} already exists in DB')


    collection_release = session.exec(select(CollectionRelease).where(CollectionRelease.version == collection_release_info["version"])).first()

    if collection_release is None:

        print(f'Creating a new collection release: {collection_release_info["name"]}:{collection_release_info["version"]}')

        collection_release = CollectionRelease(version=collection_release_info["version"],
                                               ppanggolin_version=collection_release_info["ppanggolin_version"],
                                               pangbank_wf_version=collection_release_info["pangbank_wf_version"],)

        session.add(collection_release)


    else:
        print(f'Collection release {collection.name}:{collection_release.version} already exists in DB')

        same_ppanggo_version = collection_release_info["ppanggolin_version"] == collection_release.ppanggolin_version
        same_pangbank_wf_version = collection_release_info["pangbank_wf_version"] == collection_release.pangbank_wf_version

        if not same_ppanggo_version or not same_pangbank_wf_version:
            raise ValueError(f'For collection {collection.name} release {collection_release.version}:'
                             'Not the same ppanggolin_version or pangbank_wf_version from input file and whats in the DB.. '
                             f'ppanggolin version : {collection_release_info["ppanggolin_version"]} vs {collection_release.ppanggolin_version} '
                             f'ppanggolin version : {collection_release_info["pangbank_wf_version"]} vs {collection_release.pangbank_wf_version} ')



    session.commit()
    session.refresh(collection)


    return collection_release

# def parse_pangenome_dir(pangenome_dir:Path):

    

def main():
    create_db_and_tables()
    
    collection_release_info_file = Path("tests/collection_release_info.json")

    taxonomy_source_info_file = Path("tests/taxonomy_release_info.json")

    pangenome_dir = Path("test/pangenomes")
    taxonomy_file = Path("tests/ar53_taxonomy_clean_h100.tsv")

    genome_to_taxonomy = parse_taxonomy_file(taxonomy_file)
    # #  avoir tous les genomes et leurs taxo
    # genome_to_taxonomy = {
    #     "genomeA": "Bacteria; Pseudomonadota; Gammaproteobacteria; Enterobacterales; Enterobacteriaceae; Escherichia; Escherichia coli",
    #     "genomeB": "Bacteria; Pseudomonadota; Gammaproteobacteria; Enterobacterales; Enterobacteriaceae; Escherichia; Escherichia albertii"
    # }
    
    # parse_pangenome_dir(pangenome_dir)


    # Ajouter les pangenome à partir de leur info.yaml?
    # Puis ajouter les génomes avec les genome info de chaque pangenome..
    
    with Session(engine) as session:

        collection_release = create_collection_release(collection_release_info_file, session=session)

        taxonomy_release = create_taxonomy_source(taxonomy_source_info_file, session=session)

        genomes = create_genomes_and_taxonomies(genome_to_taxonomy, taxonomy_release, session=session)

        associate_genomes_with_collection_release(genomes, collection_release, session)



if __name__ == "__main__":
    main()