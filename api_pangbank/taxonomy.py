from sqlmodel import Session, select

from .models import Genome, TaxonomySource, Taxonomy
import json
from pathlib import Path

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

def get_lineage(taxonomy:Taxonomy) -> tuple[str]:
    lineage = []

    for rank in ['domain', "phylum", "class_", "order", "family", "genus", "species", "strain"]:
        taxon_rank = getattr(taxonomy, rank)
        if taxon_rank is None:
            return tuple(lineage)
        else:
            lineage.append(taxon_rank)

    return tuple(lineage)

def build_taxonomy_dict(taxonomies: list[Taxonomy]) -> dict[tuple[str], Taxonomy]:

    taxon_dict = {}

    for taxonomy in taxonomies:
        lineage = get_lineage(taxonomy)
        taxon_dict[lineage] = taxonomy

    return taxon_dict

def parse_ranks_str(ranks_str) -> list[str]:

    ranks = [rank.strip().title() for rank in ranks_str.split(';')]

    return ranks

def create_taxonomy(lineage:tuple[str, ...], ranks:list[str], taxonomy_source:TaxonomySource, taxonomy_dict:dict[tuple[str], Taxonomy]) -> Taxonomy:

    if lineage in taxonomy_dict:
        taxonomy = taxonomy_dict[lineage]
    
    else:
        taxonomy = Taxonomy()
        for rank, taxon in zip(ranks, lineage):
            setattr(taxonomy, rank, taxon)

    return taxonomy



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


def create_genomes_and_taxonomies(genome_to_taxonomy: dict[str,str], taxonomy_source : TaxonomySource, session: Session) -> list[Genome]:


    ranks = parse_ranks_str(taxonomy_source.ranks)
    print(taxonomy_source)

    # Add new taxon from taxonomies
    existing_taxonomy_dict = build_taxonomy_dict(taxonomy_source.taxonomies)
    print(f'The taxonomy source has {len(existing_taxonomy_dict)} taxonomies')

    genomes = []
    for genome_name, taxonomy_str in genome_to_taxonomy.items():
        
        
        lineage = tuple(name.strip() for name in taxonomy_str.split(';'))

        if len(lineage) == 0:
            raise ValueError()
        taxonomy = create_taxonomy(lineage=lineage, taxonomy_source=taxonomy_source, taxonomy_dict=existing_taxonomy_dict, ranks=ranks)
        
        session.add(taxonomy)


        genome = session.exec(select(Genome).where(Genome.name == genome_name)).first()

        if genome is None:
            genome = Genome(name=genome_name) # add genome version if given?        
        else:
            print(f"Genome {genome_name} already exists. let's use it")
        
        if taxonomy not in genome.taxonomies:
            print(f"adding {taxonomy} in genome taxa.. as it does not exist yet")
            genome.taxonomies.append(taxonomy)
        else:
            print(f"{taxonomy} already exists in genome taxa.. nothing to do here")

        genomes.append(genome)
    
    session.add_all(genomes)
    session.commit()
    session.refresh(taxonomy_source)

    print(f'The taxonomy release has {len(taxonomy_source.taxonomies)} taxonomies')

    return genomes