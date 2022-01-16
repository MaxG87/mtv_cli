# Mediathekview auf der Kommandozeile
#
# Author: Bernhard Bablok, Max Görner
# License: GPL3
#
# Website: https://github.com/bablokb/mtv_cli
#

from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass
from enum import Enum
from sqlite3 import Row
from typing import Optional


class MovieQuality(str, Enum):
    HD = "HD"
    SD = "SD"
    LOW = "LOW"


@dataclass(frozen=True)
class MovieListItem:
    sender: str
    thema: str
    titel: str
    datum: Optional[dt.date]
    zeit: Optional[dt.time]
    dauer: Optional[dt.timedelta]
    groesse: int
    beschreibung: str
    url: str
    website: str
    url_untertitel: str
    url_rtmp: str
    url_klein: str
    url_rtmp_klein: str
    url_hd: str
    url_rtmp_hd: str
    datuml: Optional[int]
    url_history: str
    geo: str
    neu: bool

    class Config:
        allow_mutation = False

    @classmethod
    def from_item_list(cls, raw_entry: list[str]) -> MovieListItem:
        datum = (
            None
            if raw_entry[3] == ""
            else dt.datetime.strptime(raw_entry[3], "%d.%m.%Y").date()
        )
        zeit = (
            None
            if raw_entry[4] == ""
            else dt.datetime.strptime(raw_entry[4], "%H:%M:%S").time()
        )
        dauer_raw = (
            None
            if raw_entry[5] == ""
            else dt.datetime.strptime(raw_entry[5], "%H:%M:%S")
        )
        dauer = None if dauer_raw is None else dauer_raw - dt.datetime(1900, 1, 1)
        return MovieListItem(
            sender=raw_entry[0],
            thema=raw_entry[1],
            titel=raw_entry[2],
            datum=datum,
            zeit=zeit,
            dauer=dauer,
            groesse=int(raw_entry[6]) if raw_entry[6] else 0,
            beschreibung=raw_entry[7],
            url=raw_entry[8],
            website=raw_entry[9],
            url_untertitel=raw_entry[10],
            url_rtmp=raw_entry[11],
            url_klein=raw_entry[12],
            url_rtmp_klein=raw_entry[13],
            url_hd=raw_entry[14],
            url_rtmp_hd=raw_entry[15],
            datuml=None if raw_entry[16] == "" else int(raw_entry[16]),
            url_history=raw_entry[17],
            geo=raw_entry[18],
            neu=raw_entry[19] == "true",
        )

    @classmethod
    def from_database_row(cls, row: Row) -> MovieListItem:
        return MovieListItem(
            sender=row[0],
            thema=row[1],
            titel=row[2],
            datum=row[3],
            zeit=row[4],
            dauer=row[5],
            groesse=row[6],
            beschreibung=row[7],
            url=row[8],
            website=row[9],
            url_untertitel=row[10],
            url_rtmp=row[11],
            url_klein=row[12],
            url_rtmp_klein=row[13],
            url_hd=row[14],
            url_rtmp_hd=row[15],
            datuml=row[16],
            url_history=row[17],
            geo=row[18],
            neu=row[19],
        )

    def update(self, entry: Optional[MovieListItem]) -> MovieListItem:
        """
        Übernimm die Felder Sender und Thema, falls nötig

        Falls eines der genannten Felder leer ist, wird es von `eintrag`
        übernommen.
        """
        if entry is None:
            return self
        new = asdict(self)
        for attr in "sender", "thema":
            if not new[attr]:
                new[attr] = asdict(entry)[attr]
        return type(self)(**new)

    def dauer_as_minutes(self) -> int:
        if self.dauer is None:
            # Die Dauer des Eintrages ist unbekannt. Es wird daher ein
            # Maximalwert zurückgegeben, damit der Film nicht als zu kurz
            # aussortiert wird.
            minutes_in_day = 24 * 60
            return minutes_in_day
        return self.dauer.seconds // 60

    def get_url(self, quality: MovieQuality) -> tuple[MovieQuality, str]:
        """Bevorzugte URL zurückgeben

        Ergebnis ist (Qualität,URL)
        """
        if quality == MovieQuality.SD or not self.url_hd:
            return MovieQuality.SD, self.url

        size: MovieQuality
        if quality == MovieQuality.HD and self.url_hd:
            url_suffix = self.url_hd
            size = quality
        else:
            url_suffix = self.url_klein
            size = MovieQuality.LOW

        parts = url_suffix.split("|")
        offset = int(parts[0])
        return size, self.url[0:offset] + parts[1]
