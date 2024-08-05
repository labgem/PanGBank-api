from datetime import date
from sqlmodel import Session, select
from pathlib import Path
import json
import csv 

from .database import create_db_and_tables, engine
from .models import Collection, CollectionRelease, Genome, TaxonomySource, Pangenome, GenomePangenomeLink
from datetime import datetime

from .taxonomy import create_taxonomy_source, parse_taxonomy_file, manage_genome_taxonomies, build_taxon_dict, parse_ranks_str


# def associate_genomes_with_collection_release(genomes:list[Genome], collection_release:CollectionRelease, session:Session):

#     for genome in genomes:
#         if collection_release not in genome.collection_releases:
#             genome.collection_releases.append(collection_release)

#     session.refresh(collection_release)



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
                                               pangbank_wf_version=collection_release_info["pangbank_wf_version"],
                                               pangenomes_directory=collection_release_info["pangenomes_directory"])

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

def gather_pangenome_info(pangenome_file: Path,
                                    genomes_md5sum_file: Path,
                                    pangenome_info_file:Path,
                                    genomes_statistics_file:Path):
    pass


def parse_genomes_hash_file(genomes_md5sum_file:Path):

    with open(genomes_md5sum_file) as fl:

        for genome_info in csv.DictReader(fl, delimiter='\t'):
            print(genome_info)
            yield genome_info


def parse_pangenome_dir(pangenome_main_dir:Path, collection_release: CollectionRelease, session:Session) -> list[Pangenome]:

    pangenomes = []
    for pangenome_dir in pangenome_main_dir.iterdir():

        if not pangenome_dir.is_dir():
            continue

        pangenome_file = pangenome_dir / "pangenome.h5"
        genomes_md5sum_file = pangenome_dir / "genomes_md5sum.tsv"
        pangenome_info_file = pangenome_dir / "info.yaml"
        genomes_statistics_file = pangenome_dir / "genomes_statistics.tsv"

        # TODO: Check files exist

            
        pangenome_local_path = Path(pangenome_file.parent.name) / pangenome_file.name

        pangenome = Pangenome(file_name=pangenome_local_path.as_posix(), collection_release=collection_release)

        # Get genomes that belong to pangenome and associate them to it
        for genome_info in parse_genomes_hash_file(genomes_md5sum_file):

            genome = session.exec(select(Genome).where(Genome.name == genome_info['name'])).first()

            if genome is None:
                genome = Genome(name=genome_info['name']) # add genome version if given?        
            
            pangenome_genome_link = GenomePangenomeLink(genome=genome,
                                                        pangenome=pangenome,
                                                        genome_file_md5sum=genome_info['md5_sum'],
                                                        genome_file_name=genome_info["file_name"])
            
            pangenome.genome_links.append(pangenome_genome_link)


        session.add(pangenome)
        pangenomes.append(pangenome)

    session.commit()

    return pangenomes
    

def main():
    create_db_and_tables()
    
    collection_release_info_file = Path("tests/collection_release_info.json")

    taxonomy_source_info_file = Path("tests/taxonomy_release_info.json")

    pangenome_dir = Path("tests/pangenomes")
    taxonomy_file = Path("tests/ar53_taxonomy_clean.tsv.gz")

    genome_to_taxonomy = parse_taxonomy_file(taxonomy_file)

    
    with Session(engine) as session:

        collection_release = create_collection_release(collection_release_info_file, session=session)

        pangenomes = parse_pangenome_dir(pangenome_dir, collection_release=collection_release, session=session)

        taxonomy_source = create_taxonomy_source(taxonomy_source_info_file, session=session)

        existing_taxon_dict = build_taxon_dict(taxonomy_source.taxa)
        
        # ranks = ['domain', "phylum", "class_", "order", "family", "genus", "species", "strain"]
        ranks = parse_ranks_str(taxonomy_source.ranks)

        for pangenome in pangenomes:
            manage_genome_taxonomies(pangenome=pangenome, genome_to_taxonomy=genome_to_taxonomy, taxonomy_source=taxonomy_source, 
                                     existing_taxon_dict=existing_taxon_dict, ranks=ranks, session=session)


        # genomes = create_genomes_and_taxonomies(genome_to_taxonomy, taxonomy_release, session=session)

        # associate_genomes_with_collection_release(genomes, collection_release, session)

        


if __name__ == "__main__":
    main()