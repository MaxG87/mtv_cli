from pathlib import Path

import typer

from mtv_cli.content_retrieval import extract_entries_from_filmliste, get_lzma_fp
from mtv_cli.storage_backend import NoopDatabase

app = typer.Typer()


@app.command()
def insert_to_noop_db(filmliste: Path) -> None:
    database = NoopDatabase()
    zipped = get_lzma_fp(filmliste)
    all_movies = extract_entries_from_filmliste(zipped)
    database.insert_movies(all_movies)


@app.command()
def iterate_over_lzma_stream(filmliste: Path) -> None:
    zipped = get_lzma_fp(filmliste)
    for _ in zipped:
        pass


if __name__ == "__main__":
    app()
