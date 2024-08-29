from sqlmodel import Session, select

from app.models import Genome, TaxonomySource, Taxon, Pangenome
import json
from pathlib import Path
import gzip
from tqdm import tqdm
from collections import defaultdict
# def get_taxon_key(name: str, rank: str, parent_taxon: Taxon | None) -> str:
#     if isinstance(parent_taxon, Taxon):
#          parent_taxon_name = parent_taxon.name
#     else:
#          parent_taxon_name = ""

#     return f"{name}_{rank}_{parent_taxon_name}"

# def build_taxon_dict(taxon_list: list[Taxon]) -> dict[str, Taxon]:
#     taxon_dict = {}
#     for taxon in taxon_list:
#         key = get_taxon_key(taxon.name, taxon.rank, taxon.parent_taxon)
#         taxon_dict[key] = taxon
#     return taxon_dict

def parse_taxonomy_file(taxonomy_file:Path) -> dict[str, tuple[str]]:
    
    genome_to_lineage = {}
    
    proper_open = gzip.open if taxonomy_file.suffix == '.gz' else open

    with proper_open(taxonomy_file, 'rt') as fl:  # 'rt' mode to read as text
        for line in fl:
            genome_name, taxonomy_str = line.strip().split('\t')
            genome_to_lineage[genome_name] = tuple(name.strip() for name in taxonomy_str.split(';'))
            
    return genome_to_lineage


    genome_to_lineage = {}
    with open(taxonomy_file) as fl:
        for line in fl:
            genome_name, taxonomy = line.strip().split('\t')
            genome_to_lineage[genome_name] = taxonomy

    return genome_to_lineage

def get_lineage(taxonomy:Taxon, ranks:list[str]) -> tuple[str]:
    lineage = []

    for rank in ranks:
        taxon_rank = getattr(taxonomy, rank)
        if taxon_rank is None:
            return tuple(lineage)
        else:
            lineage.append(taxon_rank)

    return tuple(lineage)

# def build_taxonomy_dict(taxonomies: list[Taxonomy], ranks:list[str]) -> dict[tuple[str], Taxonomy]:

#     taxon_dict = {}

#     for taxonomy in taxonomies:
#         lineage = get_lineage(taxonomy, ranks)
#         taxon_dict[lineage] = taxonomy

#     return taxon_dict

def get_taxon_key(name: str, rank: str, depth: int) -> tuple[str | int, ...]:

    return tuple((rank, name, depth))

def build_taxon_dict(taxon_list: list[Taxon]) -> dict[tuple[str | int, ...], Taxon]:
    taxon_dict = {}
    for taxon in taxon_list:
        key = get_taxon_key(taxon.name, taxon.rank, taxon.depth)
        taxon_dict[key] = taxon
    return taxon_dict


def parse_ranks_str(ranks_str) -> list[str]:

    ranks = [rank.strip().title() for rank in ranks_str.split(';')]

    return ranks

def create_and_get_taxa(lineage:tuple[str, ... ], ranks:list[str], taxon_dict:dict[tuple[str|int, ...], Taxon]) -> list[Taxon]:

    assert len(ranks) >= len(lineage) 

    taxa = []
    for depth, (rank, taxon_name) in enumerate(zip(ranks, lineage)):

        taxon_key = get_taxon_key(taxon_name, rank, depth)

        if taxon_key in taxon_dict:
            # print(f'{taxon_key} in taxon_dict, reusing it')
            taxon = taxon_dict[taxon_key]
        else:
            # print(f'{taxon_key} NOT in taxon_dict, creating it')
            taxon = Taxon(name=taxon_name, rank=rank, depth=depth)
            taxon_dict[taxon_key] = taxon

        taxa.append(taxon)

    return taxa


def create_taxonomy_source(taxonomy_source_info_file : Path, session:Session) -> TaxonomySource:
     

    with open(taxonomy_source_info_file) as fl:
        taxonomy_info = json.load(fl)

    taxonomy_source_name=taxonomy_info["name"]
    taxonomy_source_version=taxonomy_info["version"]
    taxonomy_source_ranks=taxonomy_info["ranks"]

    # Check if taxonomy release already exists in DB
    statement = (
        select(TaxonomySource)
        .where(
            (TaxonomySource.name == taxonomy_source_name) &
            (TaxonomySource.version == taxonomy_source_version)
        )
    )

        
    taxonomy_source = session.exec(statement).first()

    if taxonomy_source is None:
        print('Creating a new TaxonomySource')
        taxonomy_source = TaxonomySource(name=taxonomy_source_name, version=taxonomy_source_version, ranks=taxonomy_source_ranks)

    else:
        # the taxonomy release exists already
        # checking that given ranks are identical that ones attached to the taxonomy release

        if parse_ranks_str(taxonomy_source.ranks) != parse_ranks_str(taxonomy_source_ranks):
            raise ValueError(f'Discrepancy in ranks for taxonomy_source {taxonomy_source}. '
                                f'Existing ranks : {parse_ranks_str(taxonomy_source.ranks)} vs given ranks {parse_ranks_str(taxonomy_source_ranks)}')
            
        print('taxonomy_source already exist in DB')

    session.add(taxonomy_source)

    session.commit()
    session.refresh(taxonomy_source)

    return taxonomy_source
def get_common_taxa(taxa_A:list[Taxon], taxa_B:list[Taxon]) -> list[Taxon]:

    common_taxa = []
    for taxon in taxa_A:
        if taxon in taxa_B:
            common_taxa.append(taxon)

    return common_taxa


def manage_genome_taxonomies(pangenome:Pangenome, 
                             genome_to_taxonomy: dict[str,tuple[str]],
                             taxonomy_source : TaxonomySource,
                             existing_taxon_dict:dict[tuple[str | int, ...], Taxon],
                             ranks:list[str],
                             session: Session):

    # Add new taxon from taxonomies
    existing_taxon_dict = build_taxon_dict(taxonomy_source.taxa)

    pangenome_taxa: list[Taxon] = []

    lineage_to_genomes = defaultdict(list)
    for genome_link in pangenome.genome_links:
        lineage = genome_to_taxonomy[genome_link.genome.name]
        lineage_to_genomes[lineage].append(genome_link.genome)

    
    for lineage, genomes in lineage_to_genomes.items():
        taxa = create_and_get_taxa(lineage=lineage, taxon_dict=existing_taxon_dict, ranks=ranks)
        
        if not pangenome_taxa:
            pangenome_taxa = taxa

        pangenome_taxa = get_common_taxa(taxa, pangenome_taxa)

        for taxon in taxa:
            for genome in genomes:
                if genome not in taxon.genomes:
                    taxon.genomes.append(genome)

        taxonomy_source.taxa += taxa
        
        
        session.add_all(taxa)

    pangenome.taxa = pangenome_taxa
    session.add(pangenome)
    
    session.commit()


# def create_genomes_and_taxonomies(genome_to_taxonomy: dict[str,str], taxonomy_source : TaxonomySource, session: Session) -> list[Genome]:

#     # ranks = ['domain', "phylum", "class_", "order", "family", "genus", "species", "strain"]
#     ranks = parse_ranks_str(taxonomy_source.ranks)

#     print(taxonomy_source)

#     # Add new taxon from taxonomies
#     existing_taxon_dict = build_taxon_dict(taxonomy_source.taxa)
    
#     print(f'The taxonomy source has {len(existing_taxon_dict)} taxa')

#     genomes = []

#     for genome_name, taxonomy_str in tqdm(genome_to_taxonomy.items(), total=len(genome_to_taxonomy), unit="taxon"):
        
#         lineage = tuple(name.strip() for name in taxonomy_str.split(';'))

#         taxa = create_and_get_taxa(lineage=lineage, taxon_dict=existing_taxon_dict, ranks=ranks)
        
#         taxonomy_source.taxa += taxa

#         session.add_all(taxa)

#         genome = session.exec(select(Genome).where(Genome.name == genome_name)).first()

#         if genome is None:
#             genome = Genome(name=genome_name) # add genome version if given?        
#         # else:
#         #     print(f"Genome {genome_name} already exists. let's use it")
        
#         session.add(genome)

#         for taxon in taxa:
#             if taxon not in genome.taxa:
#                 # print(f"adding taxon {taxon.name} in genome taxa.. as it does not exist yet")
#                 genome.taxa.append(taxon)
#             # else:
#             #     print(f"{taxon.name} already exists in genome taxa.. nothing to do here")

#     session.commit()
#     session.refresh(taxonomy_source)

#     print(f'The taxonomy source has {len(taxonomy_source.taxa)} taxa')


#     return genomes