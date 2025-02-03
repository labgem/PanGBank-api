from sqlmodel import Session, select
import logging
from app.models import Collection, CollectionRelease


def delete_collection(session: Session, collection_name: str) -> None:
    """
    Deletes a collection from the database if it exists.

    :param session: Database session used for querying and deleting the collection.
    :param collection_name: Name of the collection to delete.
    """
    # Query the database to find the collection with the specified name
    statement = select(Collection).where(Collection.name == collection_name)
    collection_from_db = session.exec(statement).first()

    if collection_from_db is None:
        logging.info(
            f"Collection '{collection_name}' not found in the database. Deletion aborted."
        )
    else:
        logging.info(f"Deleting collection '{collection_name}' from the database.")
        session.delete(collection_from_db)
        session.commit()


def delete_collection_release(
    session: Session, collection_name: str, release_version: str
) -> None:
    """
    Deletes a specific collection release from the database if it exists.

    :param session: Database session used for querying and deleting the collection release.
    :param collection_name: Name of the collection containing the release.
    :param release_version: Version of the collection release to delete.
    """
    # Query the database to find the collection release with the specified name and version
    statement = (
        select(CollectionRelease)
        .join(Collection)
        .where(
            (Collection.name == collection_name)
            & (CollectionRelease.version == release_version)
        )
    )
    collection_release_from_db = session.exec(statement).first()

    if collection_release_from_db is None:
        logging.info(
            f"Collection release '{collection_name}' (version: {release_version}) not found in the database. Deletion aborted."
        )
    else:
        logging.info(
            f"Deleting collection release '{collection_name}' (version: {release_version}) from the database."
        )
        session.delete(collection_release_from_db)
        session.commit()
