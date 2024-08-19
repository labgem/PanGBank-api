from datetime import date
from sqlmodel import Session, select
from pathlib import Path
import json
import csv 

from .database import create_db_and_tables, engine
from .models import Collection, CollectionRelease, Genome, TaxonomySource, Pangenome, GenomePangenomeLink, GenomeSource
from datetime import datetime

from .taxonomy import create_taxonomy_source, parse_taxonomy_file, manage_genome_taxonomies, build_taxon_dict, parse_ranks_str



# def associate_genomes_with_collection_release(genomes:list[Genome], collection_release:CollectionRelease, session:Session):

#     for genome in genomes:
#         if collection_release not in genome.collection_releases:
#             genome.collection_releases.append(collection_release)

#     session.refresh(collection_release)


def add_source_to_genomes(pangenome:Pangenome, genome_sources:list[GenomeSource], genome_to_source:dict[str,str], session:Session):


    source_name_to_genome_source = {genome_source.name:genome_source for genome_source in genome_sources}
    for genome_link in pangenome.genome_links:

        source_name = genome_to_source[genome_link.genome.name]
        genome_source = source_name_to_genome_source[source_name]
        genome_link.genome.genome_source = genome_source

    
    session.add_all(genome_sources)

    session.commit()



def parse_genome_to_source_files(source_genomes_files: list[Path]) -> dict[str, str]:
    
    genome_to_source = {}
    
    for genome_to_source_file in source_genomes_files:
        
        with open(genome_to_source_file) as fl:
            source = genome_to_source_file.stem
            genome_to_source.update({line.strip():source for line in fl})
    
    return genome_to_source
        

def create_genome_sources(genome_sources_info_file : Path, session:Session) -> list[GenomeSource]:
     
    genome_sources = []

    with open(genome_sources_info_file) as fl:
        genome_sources_info = json.load(fl)

    for genome_info in genome_sources_info:
        # Check if taxonomy release already exists in DB
        statement = (
            select(GenomeSource)
            .where(
                (GenomeSource.name == genome_info['name'])
            )
        )

        genome_source = session.exec(statement).first()

        if genome_source is None:
            print('Creating a new GenomeSource')
            genome_source = GenomeSource(**genome_info)

        else:            
            print('GenomeSource already exist in DB')

        session.add(genome_source)
        genome_sources.append(genome_source)

    session.commit()

    for genome_source in genome_sources:
        session.refresh(genome_source)

    return genome_sources


def create_collection_release(collection_release_info_file:Path, session:Session) -> CollectionRelease:
    
    with open(collection_release_info_file) as fl:
        collection_release_info = json.load(fl)

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


    statement =  select(CollectionRelease).join(Collection).where(
        (Collection.name == collection_release_info["name"] ) &
        (CollectionRelease.version == collection_release_info["version"])
        )
    
    collection_release = session.exec(statement).first()

    if collection_release is None:

        print(f'Creating a new collection release: {collection_release_info["name"]}:{collection_release_info["version"]}')

        collection_release = CollectionRelease(version=collection_release_info["version"],
                                               ppanggolin_version=collection_release_info["ppanggolin_version"],
                                               pangbank_wf_version=collection_release_info["pangbank_wf_version"],
                                               pangenomes_directory=collection_release_info["pangenomes_directory"],
                                               collection=collection)

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
        
        pangenome = session.exec(select(Pangenome).where((Pangenome.file_name == pangenome_local_path.as_posix()) and (Pangenome.collection_release == collection_release) )).first()

        if pangenome is None:
            pangenome = Pangenome(file_name=pangenome_local_path.as_posix(), collection_release=collection_release)
            session.add(pangenome)

        # Get genomes that belong to pangenome and associate them to it
        for genome_info in parse_genomes_hash_file(genomes_md5sum_file):

            genome = session.exec(select(Genome).where(Genome.name == genome_info['name'])).first()

            if genome is None:
                genome = Genome(name=genome_info['name']) # add genome version if given?
                session.add(genome)

            
            genome_pangenome_link = session.exec(select(GenomePangenomeLink).where((GenomePangenomeLink.genome == genome) and (GenomePangenomeLink.pangenome == pangenome) )).first()
            
            if genome_pangenome_link is None:
                pangenome_genome_link = GenomePangenomeLink(genome=genome,
                                                            pangenome=pangenome,
                                                            genome_file_md5sum=genome_info['md5_sum'],
                                                            genome_file_name=genome_info["file_name"])
                session.add(pangenome_genome_link)
                
                pangenome.genome_links.append(pangenome_genome_link)

        pangenomes.append(pangenome)

    session.commit()
    session.refresh(collection_release)

    return pangenomes
    

def main():
    create_db_and_tables()
    
    collection_release_info_file = Path("tests/collection_release_info.json")

    taxonomy_source_info_file = Path("tests/taxonomy_release_info.json")

    genome_sources_info_file = Path("tests/genome_source_info.json")

    source_genomes_files = [Path('tests/GenBank.list'), Path('tests/RefSeq.list')]

    pangenome_dir = Path("tests/pangenomes")
    taxonomy_file = Path("tests/ar53_taxonomy_clean.tsv.gz")



    genome_to_taxonomy = parse_taxonomy_file(taxonomy_file)

    
    with Session(engine) as session:

        collection_release = create_collection_release(collection_release_info_file, session=session)

        print(f'The collection release has {len(collection_release.pangenomes)} pangenomes')

        pangenomes = parse_pangenome_dir(pangenome_dir, collection_release=collection_release, session=session)

        taxonomy_source = create_taxonomy_source(taxonomy_source_info_file, session=session)

        print(f'The taxonomy source has {len(taxonomy_source.taxa)} taxa')
        
        existing_taxon_dict = build_taxon_dict(taxonomy_source.taxa)
        
        # ranks = ['domain', "phylum", "class_", "order", "family", "genus", "species", "strain"]
        ranks = parse_ranks_str(taxonomy_source.ranks)

        for pangenome in pangenomes:
            manage_genome_taxonomies(pangenome=pangenome, genome_to_taxonomy=genome_to_taxonomy, taxonomy_source=taxonomy_source, 
                                     existing_taxon_dict=existing_taxon_dict, ranks=ranks, session=session)
            

        genome_sources = create_genome_sources(genome_sources_info_file, session=session)
        print(genome_sources)

        genome_to_source = parse_genome_to_source_files(source_genomes_files)

        for pangenome in pangenomes:
            add_source_to_genomes(pangenome, genome_sources, genome_to_source, session)

        print(f'The taxonomy source {taxonomy_source.name}-{taxonomy_source.version} has {len(taxonomy_source.taxa)} taxa')
        print(f'The collection release {collection_release.collection.name}-{collection_release.version} has {len(collection_release.pangenomes)} pangenomes')

        for genome_source in genome_sources:
            print(f'The genome source {genome_source.name} has {len(genome_source.genomes)} genomes')



        # genomes = create_genomes_and_taxonomies(genome_to_taxonomy, taxonomy_release, session=session)

        # associate_genomes_with_collection_release(genomes, collection_release, session)

        


if __name__ == "__main__":
    main()