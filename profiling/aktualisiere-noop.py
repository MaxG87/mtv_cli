import typer

from pathlib import Path

from mtv_cli.storage_backend import NoopDatabase
from mtv_cli.content_retrieval import extract_entries_from_filmliste, get_lzma_fp


def main(filmliste: Path) -> None:
    database = NoopDatabase()
    zipped = get_lzma_fp(filmliste)
    all_movies = extract_entries_from_filmliste(zipped)
    database.insert_movies(all_movies)


if __name__ == "__main__":
    typer.run(main)
