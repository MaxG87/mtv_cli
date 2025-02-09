#!/usr/bin/env python3
# Mediathekview auf der Kommandozeile
#
# Author: Bernhard Bablok, Max Görner
# License: GPL3
#
# Website: https://github.com/bablokb/mtv_cli
#

from __future__ import annotations

import configparser
import datetime as dt
import re
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, Optional, TextIO

import typer
from loguru import logger
from pick import pick

from mtv_cli.constants import (
    FILME_SQLITE,
    MTV_CLI_CONFIG,
    MTV_CLI_HOME,
    SEL_FORMAT,
    SEL_TITEL,
    URL_FILMLISTE,
)
from mtv_cli.content_retrieval import (
    FilmDownloadFehlerhaft,
    LowMemoryFileSystemDownloader,
    extract_entries_from_filmliste,
    get_lzma_fp,
    get_url_fp,
)
from mtv_cli.film import MovieListItem, MovieQuality
from mtv_cli.film_filter import AgeDurationFilter
from mtv_cli.storage_backend import DownloadStatus, FilmDB

app = typer.Typer(name="mtv-cli")

BATCH_PROCESSING_OPTION = typer.Option(False, help="Aktiviere Nicht-Interaktiven Modus")
CONFIG_OPTION = typer.Option(MTV_CLI_CONFIG, exists=True, help="Konfigurationsdatei")
DBFILE_OPTION = typer.Option(
    FILME_SQLITE, exists=True, help="Datei mit SQLITE-Datenbankdatei"
)
LOGLEVEL_OPTION = typer.Option(None, help="Level für Logausgabe")
MAYBE_DBFILE_OPTION = typer.Option(FILME_SQLITE, help="Datei mit SQLITE-Datenbankdatei")
MAYBE_QUALITY_OPTION = typer.Option(None, help="Gewünschte Filmqualität.")
QUERY_ARG = typer.Argument(None, help="Suchausdrücke")


def setup_logging(level: Optional[str], config) -> None:
    logger.remove()
    log_level: str = level if level else config["MSG_LEVEL"]
    logger.add(sys.stderr, level=log_level)


def load_configuration(config_f: Path) -> dict[str, Any]:
    if not config_f.exists():
        sys.exit("Konfigurationsdatei nicht vorhanden!")
    parser = configparser.RawConfigParser()
    parser.read(config_f)
    try:
        cfg = {
            "MAX_ALTER": parser.getint("CONFIG", "MAX_ALTER"),
            "MIN_DAUER": parser.getint("CONFIG", "MIN_DAUER"),
            "MSG_LEVEL": parser.get("CONFIG", "MSG_LEVEL"),
            "QUALITAET": MovieQuality(parser.get("CONFIG", "QUALITAET")),
            "ZIEL_DOWNLOADS": Path(parser.get("CONFIG", "ZIEL_DOWNLOADS")).expanduser(),
        }
    except Exception as e:
        sys.exit(f"Konfiguration fehlerhaft! Fehler: {e}")
    zielordner = cfg["ZIEL_DOWNLOADS"]
    if not zielordner.is_dir():  # type: ignore[attr-defined]
        sys.exit(f"Der Zielordner >>{zielordner}<< für Filme existiert nicht.")

    return cfg


class Options:
    pass


@app.command()
def aktualisiere_filmliste(
    config: Path = CONFIG_OPTION,
    dbfile: Path = MAYBE_DBFILE_OPTION,
    quelle: str = typer.Option(
        URL_FILMLISTE,
        help="Quelle für neue Filmliste. Erlaubte Werte sind auto|json|Url|Datei.",
    ),
    log_level: str = LOGLEVEL_OPTION,
) -> None:
    """Update der Filmliste"""
    # TODO: Führe UpdateSource als ContextManager ein
    cfg = load_configuration(config)
    setup_logging(log_level, cfg)
    film_filter = AgeDurationFilter(
        max_age=cfg["MAX_ALTER"],
        min_duration=cfg["MIN_DAUER"],
    )

    fh = get_update_source_file_handle(quelle)
    all_movies = extract_entries_from_filmliste(fh)
    relevant_movies = (movie for movie in all_movies if film_filter.is_permitted(movie))

    filmDB = FilmDB(dbfile)
    filmDB.insert_movies(relevant_movies)


def get_update_source_file_handle(update_source: str) -> TextIO:
    if update_source == "auto":
        src = URL_FILMLISTE
    elif update_source == "json":
        # existierende Filmliste verwenden
        src = str(MTV_CLI_HOME / "filme.json")
    else:
        src = update_source

    if src.startswith("http"):
        return get_lzma_fp(get_url_fp(src))
    else:
        return open(src, "r", encoding="utf-8")


def get_suche() -> Iterable[str]:
    suche_titel = "Auswahl Suchdetails"
    suche_opts = [
        "Weiter",
        "Global []",
        "Sender []",
        "Datum []",
        "Thema []",
        "Titel []",
        "Beschreibung []",
    ]
    while True:
        # suche_opts anzeigen
        # mit readline Suchebegriff abfragen, speichern in suche_wert
        # break, falls Auswahl "Ende"
        selection = pick(suche_opts, suche_titel)
        if len(selection) == 0:
            break
        elif len(selection) == 1:
            option, index = selection[0]
            begriff = input("Suchbegriff: ")
            pos = option.find("[")
            suche_opts[index] = option[0:pos] + " [" + begriff + "]"
        else:
            continue

    # Ergebnis extrahieren
    square_brackets_split = re.compile(r"\[|\]")
    if len(suche_opts[1]) > len("Global []"):
        yield square_brackets_split.split(suche_opts[1])[1]
    else:
        for opt in suche_opts[2:]:
            token = square_brackets_split.split(opt)
            if len(token[1]) > 0:
                yield token[0].strip() + ":" + token[1]


def get_select(filme: list[MovieListItem]) -> Iterable[str]:
    for film in filme:
        sender = film.sender
        thema = film.thema
        titel = film.titel
        datum = "" if film.datum is None else film.datum.isoformat()
        dauer = film.dauer_as_minutes()
        yield SEL_FORMAT.format(sender, thema, datum, dauer, titel)


def filme_suchen(query: Optional[list[str]], filmDB: FilmDB) -> Iterable[MovieListItem]:
    """Filme gemäß Vorgabe suchen"""
    if query is None:
        query = list(get_suche())
    return filmDB.finde_filme(query)


def zeige_liste(filme: list[MovieListItem]) -> list[tuple[str, int]]:
    """Filmliste anzeigen, Auswahl zurückgeben"""
    title = f"  {SEL_TITEL}"
    preselection = list(get_select(filme))
    selection: list[tuple[str, int]] = pick(preselection, title, multiselect=True)
    return selection


@app.command()
def filme_vormerken(
    config: Path = CONFIG_OPTION,
    dbfile: Path = DBFILE_OPTION,
    log_level: Optional[str] = LOGLEVEL_OPTION,
    suche: Optional[list[str]] = QUERY_ARG,
):
    """Filmliste anzeigen, Auswahl für späteren Download speichern"""
    options = load_configuration(config)
    setup_logging(log_level, options)
    filmDB: FilmDB = FilmDB(dbfile)
    selected_filme = list(
        select_movies_for_download(suche, filmDB=filmDB, do_batch=False)
    )

    total = len(selected_filme)
    num_changes = filmDB.save_downloads(selected_filme, status="V")
    logger.info(f"{num_changes} von {total} Filme vorgemerkt für Download")
    return num_changes


@app.command()
def sofort_herunterladen(
    config: Path = CONFIG_OPTION,
    dbfile: Path = DBFILE_OPTION,
    log_level: Optional[str] = LOGLEVEL_OPTION,
    qualitaet: Optional[MovieQuality] = MAYBE_QUALITY_OPTION,
    suche: Optional[list[str]] = QUERY_ARG,
) -> None:
    """Filmliste anzeigen, sofortiger Download nach Auswahl"""
    options = load_configuration(config)
    setup_logging(log_level, options)
    filmDB = FilmDB(dbfile)
    zielordner: Path = options["ZIEL_DOWNLOADS"]
    qualitaet = options["QUALITAET"] if qualitaet is None else qualitaet
    retriever = LowMemoryFileSystemDownloader(root=zielordner, quality=qualitaet)

    selected_movies = select_movies_for_download(suche, filmDB=filmDB, do_batch=False)
    for film in selected_movies:
        logger.info(f"About to download {film}.")
        retriever.download_film(film)


def select_movies_for_download(
    query: Optional[list[str]], do_batch: bool, filmDB: FilmDB
) -> Iterable[MovieListItem]:
    filme = list(filme_suchen(query, filmDB))
    if len(filme) == 0:
        logger.info("Keine Suchtreffer")
        return 0

    if do_batch:
        selection_ids = set(range(len(filme)))
    else:
        selection_ids = {idx for (_, idx) in zeige_liste(filme)}

    for n, film in enumerate(filme):
        if n in selection_ids:
            yield film


@app.command()
def vormerkungen_herunterladen(
    config: Path = CONFIG_OPTION,
    dbfile: Path = DBFILE_OPTION,
    log_level: Optional[str] = LOGLEVEL_OPTION,
    qualitaet: Optional[MovieQuality] = MAYBE_QUALITY_OPTION,
) -> None:
    """Download vorgemerkter Filme"""
    options = load_configuration(config)
    setup_logging(log_level, options)
    zielordner: Path = options["ZIEL_DOWNLOADS"]
    filmDB: FilmDB = FilmDB(dbfile)
    qualitaet = options["QUALITAET"] if qualitaet is None else qualitaet
    retriever = LowMemoryFileSystemDownloader(root=zielordner, quality=qualitaet)

    selected_movies = list(filmDB.read_downloads(status=["V", "F"]))
    if len(selected_movies) == 0:
        logger.info("Keine vorgemerkten Filme vorhanden")
        return
    for film, _, _ in selected_movies:
        logger.info(f"About to download {film}.")
        try:
            retriever.download_film(film)
            download_was_successful = True
        except FilmDownloadFehlerhaft:
            download_was_successful = False
        filmDB.update_downloads(film, "K" if download_was_successful else "F")
    filmDB.save_status("_download")


@app.command()
def suche(
    config: Path = CONFIG_OPTION,
    dbfile: Path = DBFILE_OPTION,
    stapelverarbeitung: bool = BATCH_PROCESSING_OPTION,
    log_level: str = LOGLEVEL_OPTION,
    suche: Optional[list[str]] = QUERY_ARG,
) -> None:
    """Suche Film ohne diesen herunterzuladen"""
    options = load_configuration(config)
    setup_logging(log_level, options)
    filmDB = FilmDB(dbfile)
    filme = list(filme_suchen(suche, filmDB))
    if len(filme) == 0:
        return

    if stapelverarbeitung:
        print("[")
        for film in filme:
            print(asdict(film), end=",")
        print("]")
    else:
        print(SEL_TITEL)
        print(len(SEL_TITEL) * "_")
        for line in get_select(filme):
            print(line)


@app.command()
def entferne_filmvormerkungen(
    config: Path = CONFIG_OPTION,
    dbfile: Path = DBFILE_OPTION,
    log_level: Optional[str] = LOGLEVEL_OPTION,
) -> None:
    """Entferne Vormerkungen für Filme"""
    options = load_configuration(config)
    setup_logging(log_level, options)

    # Syntax Reminder: {keyword:minimum length:maximum length}
    DLL_FORMAT = "|".join(
        [
            "{status:1.1}",
            "{datumstatus:8.8}",
            "{sender:7.7}",
            "{thema:8.8}",
            "{sendedatum:8.8}",
            "{dauer:4}",
            "{titel:58.58}",
        ]
    )
    DLL_TITEL = ("St" + DLL_FORMAT).format(
        status="a",
        datumstatus="S-Datum",
        sender="Sender",
        thema="Thema",
        sendedatum="Datum",
        dauer="Dauer",
        titel="Titel",
    )

    def format_download_row(arg: tuple[MovieListItem, DownloadStatus, dt.date]) -> str:
        film, status, datumstatus = arg
        sendedatum = (
            "Unbekannt" if film.datum is None else film.datum.strftime("%d.%m.%y")
        )
        return DLL_FORMAT.format(
            status=status,
            datumstatus=datumstatus.strftime("%d.%m.%y"),
            sender=film.sender,
            thema=film.thema,
            sendedatum=sendedatum,
            dauer=film.dauer_as_minutes(),
            titel=film.titel,
        )

    # Liste lesen
    filmDB: FilmDB = FilmDB(dbfile)
    filme = list(filmDB.read_downloads())
    if len(filme) == 0:
        logger.info("Keine vorgemerkten Filme vorhanden")
        return

    # Liste aufbereiten
    selected = pick(
        filme, DLL_TITEL, multiselect=True, options_map_func=format_download_row
    )

    # IDs extrahieren und Daten löschen
    deletes = []
    for sel_text, sel_index in selected:
        film = filme[sel_index][0]
        deletes.append(film)
    changes = filmDB.delete_downloads(deletes)
    logger.info("%d vorgemerkte Filme gelöscht" % changes)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
