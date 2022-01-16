# Mediathekview auf der Kommandozeile
#
# Methoden rund um den Download
#
# Author: Bernhard Bablok, Max Görner
# License: GPL3
#
# Website: https://github.com/bablokb/mtv_cli
#

import lzma
import urllib.request as request
from pathlib import Path
from typing import Iterable, Optional, TextIO

import ijson  # type: ignore[import]
import requests
from loguru import logger
from pydantic import BaseModel

from mtv_cli.film import MovieListItem, MovieQuality


class FilmDownloadFehlerhaft(RuntimeError):
    pass


class LowMemoryFileSystemDownloader(BaseModel):
    root: Path
    quality: MovieQuality
    chunk_size: int = 1024 * 1024  # 1 MiB

    def get_filename(self, film: MovieListItem) -> Path:
        # Infos zusammensuchen
        _, url = film.get_url(self.quality)
        thema = film.thema.replace("/", "_")
        titel = film.titel.replace("/", "_")
        ext = url.split(".")[-1].lower()
        fname = self.root / f"{film.sender}_{film.datum}_{thema}_{titel}.{ext}"
        return fname

    def download_film(self, film: MovieListItem) -> None:
        """Download eines einzelnen Films"""
        real_quality, url = film.get_url(self.quality)
        if real_quality != self.quality:
            logger.warning(
                f"Angeforderte Qualität {self.quality} ist für Film {film} nicht"
                " vorhanden! Nutze stattdessen {real_quality}."
            )
        response = requests.get(url, stream=True)
        try:
            response.raise_for_status()
            with self.get_filename(film).open("wb") as fh:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    fh.write(chunk)
        except requests.HTTPError as http_err:
            logger.error(f"Download des Films {film} ist fehlgeschlagen!")
            logger.exception(http_err)
            raise FilmDownloadFehlerhaft from http_err


def get_url_fp(url):
    """URL öffnen und Filepointer zurückgeben"""
    return request.urlopen(url)


def get_lzma_fp(url_fp) -> TextIO:
    """Filepointer des LZMA-Entpackers. Argument ist der FP der URL"""
    ret: TextIO = lzma.open(url_fp, "rt", encoding="utf-8")
    return ret


def extract_entries_from_filmliste(fh: TextIO) -> Iterable[MovieListItem]:
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
    last_entry: Optional[MovieListItem] = None
    for cur_item in stream:
        if cur_item == start_item:
            raw_entry: list[str] = []
            entry_has_started = True
        elif cur_item == end_item:
            entry_has_started = False
            cur_entry = MovieListItem.from_item_list(raw_entry).update(last_entry)
            last_entry = cur_entry
            yield cur_entry
        elif entry_has_started:
            raw_entry.append(cur_item[-1])
