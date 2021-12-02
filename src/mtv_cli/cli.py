#!/usr/bin/env python3
# Mediathekview auf der Kommandozeile
#
# Author: Bernhard Bablok, Max Görner
# License: GPL3
#
# Website: https://github.com/bablokb/mtv_cli
#

from __future__ import annotations

import typer
import configparser
import datetime as dt
import fcntl
import lzma
import re
import sys
import urllib.request as request
from argparse import ArgumentParser
from pathlib import Path
from typing import Iterable, Optional, TextIO

import ijson  # type: ignore[import]
from loguru import logger
from pick import pick

from mtv_cli.constants import (
    DLL_FORMAT,
    DLL_TITEL,
    FILME_SQLITE,
    MTV_CLI_CONFIG,
    MTV_CLI_HOME,
    SEL_FORMAT,
    SEL_TITEL,
    URL_FILMLISTE,
    VERSION,
)
from mtv_cli.content_retrieval import (
    FilmDownloadFehlerhaft,
    LowMemoryFileSystemDownloader,
)
from mtv_cli.film import FilmlistenEintrag, FILM_QUALITAET
from mtv_cli.film_filter import AgeDurationFilter, FilmFilter
from mtv_cli.storage_backend import DownloadStatus, FilmDB

app = typer.Typer(name="mtv-cli")

CONFIG_OPTION = typer.Option(MTV_CLI_CONFIG, exists=True)
DBFILE_OPTION = typer.Option(FILME_SQLITE, help="Datei mit SQLITE-Datenbankdatei")
BATCH_PROCESSING_OPTION = typer.Option(False, help="Aktiviere Nicht-Interaktiven Modus")


class Options:
    pass


def get_url_fp(url):
    """URL öffnen und Filepointer zurückgeben"""
    return request.urlopen(url)


def get_lzma_fp(url_fp) -> TextIO:
    """Filepointer des LZMA-Entpackers. Argument ist der FP der URL"""
    ret: TextIO = lzma.open(url_fp, "rt", encoding="utf-8")
    return ret


def extract_entries_from_filmliste(fh: TextIO) -> Iterable[FilmlistenEintrag]:
    """
    Extrahiere einzelne Einträge aus MediathekViews Filmliste

    Diese Funktion nimmt eine IO-Objekt und extrahiert aus diesem einzelne
    Filmeinträge. Es wird darauf geachtet, dabei möglichst sparsam mit dem
    Arbeitsspeicher umzugehen.
    """
    stream = ijson.parse(fh)
    start_item = ("X", "start_array", None)
    end_item = ("X", "end_array", None)
    entry_has_started = False
    last_entry: Optional[FilmlistenEintrag] = None
    for cur_item in stream:
        if cur_item == start_item:
            raw_entry: list[str] = []
            entry_has_started = True
        elif cur_item == end_item:
            entry_has_started = False
            cur_entry = FilmlistenEintrag.from_item_list(raw_entry).update(last_entry)
            last_entry = cur_entry
            yield cur_entry
        elif entry_has_started:
            raw_entry.append(cur_item[-1])


@app.command()
def aktualisiere_filmliste(
    config: Path = CONFIG_OPTION,
    dbfile: Path = DBFILE_OPTION,
    quelle: str = typer.Option(
        URL_FILMLISTE,
        help="Quelle für neue Filmliste. Erlaubte Werte sind auto|json|Url|Datei.",
    ),
) -> None:
    """Update der Filmliste"""
    # TODO: Führe UpdateSource als ContextManager ein
    fh = get_update_source_file_handle(quelle)
    entry_candidates = extract_entries_from_filmliste(fh)

    filmDB: FilmDB = FilmDB(dbfile)
    filmDB.create_filmtable()
    filmDB.cursor.execute("BEGIN;")
    film_filter: FilmFilter = options.film_filter
    for entry in entry_candidates:
        if film_filter.is_permitted(entry):
            filmDB.insert_film(entry)
    filmDB.commit()
    filmDB.save_filmtable()

    fh.close()


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


def get_select(filme: list[FilmlistenEintrag]) -> Iterable[str]:
    for film in filme:
        sender = film.sender
        thema = film.thema
        titel = film.titel
        datum = "" if film.datum is None else film.datum.isoformat()
        dauer = film.dauer_as_minutes()
        yield SEL_FORMAT.format(sender, thema, datum, dauer, titel)


def filme_suchen(options) -> Iterable[FilmlistenEintrag]:
    """Filme gemäß Vorgabe suchen"""
    if not options.suche:
        options.suche = list(get_suche())

    criteria: list[str] = options.suche
    filmDB: FilmDB = options.filmDB
    return filmDB.finde_filme(criteria)


def zeige_liste(filme: list[FilmlistenEintrag]) -> list[tuple[str, int]]:
    """Filmliste anzeigen, Auswahl zurückgeben"""
    title = f"  {SEL_TITEL}"
    preselection = list(get_select(filme))
    selection: list[tuple[str, int]] = pick(preselection, title, multiselect=True)
    return selection


@app.command()
def filme_vormerken(dbfile: Path = DBFILE_OPTION):
    """Filmliste anzeigen, Auswahl für späteren Download speichern"""
    filmDB: FilmDB = FilmDB(dbfile)
    selected_filme = list(select_movies_for_download(options))

    total = len(selected_filme)
    num_changes = filmDB.save_downloads(selected_filme, status="V")
    logger.info(f"{num_changes} von {total} Filme vorgemerkt für Download")
    return num_changes


@app.command()
def sofort_herunterladen(
    dbfile: Path = DBFILE_OPTION,
    zielordner: Path = typer.Option(
        Path("~/Videos"), help="Zielordner für heruntergeladene Filme."
    ),
    qualitaet: FILM_QUALITAET = typer.Option(
        "HD", help="Gewünschte Filmqualität. Erlaubte Werte: LOW|SD|HD"
    ),
) -> None:
    """Filmliste anzeigen, sofortiger Download nach Auswahl"""
    retriever = LowMemoryFileSystemDownloader(
        root=zielordner.expanduser(),
        quality=qualitaet,
    )

    selected_movies = select_movies_for_download(options)
    for film in selected_movies:
        logger.info(f"About to download {film}.")
        retriever.download_film(film)


def select_movies_for_download(options, do_batch: bool) -> Iterable[FilmlistenEintrag]:
    filme = list(filme_suchen(options))
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
    options,
    dbfile: Path = DBFILE_OPTION,
    zielordner: Path = typer.Option(
        Path("~/Videos"), help="Zielordner für heruntergeladene Filme."
    ),
    qualitaet: FILM_QUALITAET = typer.Option(
        "HD", help="Gewünschte Filmqualität. Erlaubte Werte: LOW|SD|HD"
    ),
) -> None:
    """Download vorgemerkter Filme"""
    filmDB: FilmDB = FilmDB(dbfile)
    retriever = LowMemoryFileSystemDownloader(
        root=zielordner.expanduser(),
        quality=qualitaet,
    )

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
def suche(stapelverarbeitung: BATCH_PROCESSING_OPTION) -> None:
    """Suche Film ohne diesen herunterzuladen"""

    filme = list(filme_suchen(options))
    if len(filme) == 0:
        return False

    if stapelverarbeitung:
        print("[")
        for film in filme:
            print(film.dict(), end=",")
        print("]")
    else:
        print(SEL_TITEL)
        print(len(SEL_TITEL) * "_")
        for line in get_select(filme):
            print(line)
    return True


@app.command()
def entferne_filmvormerkungen(
    dbfile: Path = DBFILE_OPTION,
) -> None:
    """Entferne Vormerkungen für Filme"""

    def format_download_row(
        arg: tuple[FilmlistenEintrag, DownloadStatus, dt.date]
    ) -> str:
        film, status, datumstatus = arg
        return DLL_FORMAT.format(
            status,
            datumstatus.strftime("%d.%m.%y"),
            film.sender,
            film.thema,
            "Unbekannt" if film.datum is None else film.datum.strftime("%d.%m.%y"),
            film.dauer_as_minutes(),
            film.titel,
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


def get_parser():
    parser = ArgumentParser(
        add_help=False, description="Mediathekview auf der Kommandozeile"
    )

    parser.add_argument(
        "-V",
        "--vormerken",
        action="store_true",
        dest="doLater",
        help="Filmauswahl im Vormerk-Modus",
    )
    parser.add_argument(
        "-S",
        "--sofort",
        action="store_true",
        dest="doNow",
        help="Filmauswahl im Sofort-Modus",
    )

    parser.add_argument(
        "-D",
        "--download",
        action="store_true",
        dest="doDownload",
        help="Vorgemerkte Filme herunterladen",
    )
    parser.add_argument(
        "-Q", "--query", action="store_true", dest="doSearch", help="Filme suchen"
    )

    parser.add_argument(
        "-b",
        "--batch",
        action="store_true",
        dest="doBatch",
        help="Ausführung ohne User-Interface (zusammen mit -V, -Q und -S)",
    )
    parser.add_argument(
        "-d",
        "--db",
        metavar="Datei",
        dest="dbfile",
        default=FILME_SQLITE,
        help="Datenbankdatei",
        type=Path,
    )

    parser.add_argument(
        "-l",
        "--level",
        metavar="Log-Level",
        dest="level",
        default=None,
        help="Meldungen ab angegebenen Level ausgeben",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        dest="doVersionInfo",
        help="Ausgabe Versionsnummer",
    )
    parser.add_argument("-h", "--hilfe", action="help", help="Diese Hilfe ausgeben")

    parser.add_argument("suche", nargs="*", metavar="Suchausdruck", help="Suchausdruck")
    return parser


def get_lock(datei: Path):
    global fd_datei

    if not datei.is_file():
        return True

    fd_datei = datei.open("r")
    try:
        fcntl.flock(fd_datei, fcntl.LOCK_NB | fcntl.LOCK_EX)
        return True
    except IOError:
        return False


def get_config(parser):
    return {
        "MSG_LEVEL": parser.get("CONFIG", "MSG_LEVEL"),
        "MAX_ALTER": parser.getint("CONFIG", "MAX_ALTER"),
        "MIN_DAUER": parser.getint("CONFIG", "MIN_DAUER"),
        "NUM_DOWNLOADS": parser.getint("CONFIG", "NUM_DOWNLOADS"),
        "ZIEL_DOWNLOADS": parser.get("CONFIG", "ZIEL_DOWNLOADS"),
        "CMD_DOWNLOADS": parser.get("CONFIG", "CMD_DOWNLOADS"),
        "CMD_DOWNLOADS_M3U": parser.get("CONFIG", "CMD_DOWNLOADS_M3U"),
        "QUALITAET": parser.get("CONFIG", "QUALITAET"),
    }


def main() -> None:
    if not MTV_CLI_CONFIG.exists():
        sys.exit("Konfigurationsdatei nicht vorhanden!")
    config_parser = configparser.RawConfigParser()
    config_parser.read(MTV_CLI_CONFIG)
    try:
        config = get_config(config_parser)
    except Exception as e:
        sys.exit(f"Konfiguration fehlerhaft! Fehler: {e}")

    opt_parser = get_parser()
    options = opt_parser.parse_args(namespace=Options)
    if options.doVersionInfo:
        print("Version: %s" % VERSION)
        sys.exit(0)

    logger.remove()
    log_level: str = options.level if options.level else config["MSG_LEVEL"]
    logger.add(sys.stderr, level=log_level)

    # Verzeichnis HOME/.mediathek3 anlegen
    MTV_CLI_HOME.mkdir(parents=True, exist_ok=True)

    if not options.upd_src and not options.dbfile.is_file():
        logger.error("Datenbank %s existiert nicht!" % options.dbfile)
        sys.exit(3)

    # Lock anfordern
    if not get_lock(options.dbfile):
        logger.error("Datenbank %s ist gesperrt" % options.dbfile)
        sys.exit(3)

    # Globale Objekte anlegen
    options.config = config
    options.filmDB = FilmDB(dbfile=options.dbfile)
    options.film_filter = AgeDurationFilter(
        max_age=config["MAX_ALTER"],
        min_duration=config["MIN_DAUER"],
    )
    app()


if __name__ == "__main__":
    main()
