from __future__ import annotations

import calendar
import csv
import io
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Iterator

NANODEGREES = 1_000_000_000
CENTER_LATITUDE = 41_474_937_000
CENTER_LONGITUDE = -74_913_585_000
Q1_LATITUDE_STEP = 4_491_556
Q1_LONGITUDE_STEP = 5_986_000
Q2_LATITUDE_STEP = Q1_LATITUDE_STEP // 2
Q2_LONGITUDE_STEP = Q1_LONGITUDE_STEP // 2
CELL_SHIFT = 10
CELL_MASK = (1 << CELL_SHIFT) - 1
ROUTE_SHIFT = 20

PREPARED_COLUMNS = (
    "schema_version",
    "dataset_id",
    "event_id",
    "source_line_number",
    "month_id",
    "trip_source_checksum",
    "trip_original_row_number",
    "fare_source_checksum",
    "fare_original_row_number",
    "joined_record_id",
    "medallion",
    "hack_license",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "rate_code",
    "store_and_fwd_flag",
    "passenger_count",
    "trip_time_seconds",
    "trip_distance_miles",
    "pickup_longitude",
    "pickup_latitude",
    "dropoff_longitude",
    "dropoff_latitude",
    "payment_type",
    "fare_amount",
    "surcharge",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "total_amount",
)

@dataclass(slots=True, frozen=True)
class Event:
    source_line: int
    event_id: str
    event_time: int
    medallion: str
    pickup_q1: int | None
    dropoff_q1: int | None
    pickup_q2: int | None
    dropoff_q2: int | None
    fare_plus_tip_cents: int | None


def parse_decimal_nano(value: str) -> int | None:
    text = value.strip()
    if not text:
        return None
    negative = text.startswith("-")
    if text[:1] in {"-", "+"}:
        text = text[1:]
    whole, separator, fraction = text.partition(".")
    if not whole or not whole.isdigit() or (separator and not fraction.isdigit()):
        return None
    if len(fraction) > 9:
        return None
    scaled = int(whole) * NANODEGREES + int((fraction + "000000000")[:9])
    return -scaled if negative else scaled


def parse_cents(value: str) -> int | None:
    text = value.strip()
    if not text:
        return None
    negative = text.startswith("-")
    if text[:1] in {"-", "+"}:
        text = text[1:]
    whole, separator, fraction = text.partition(".")
    if not whole or not whole.isdigit() or (separator and not fraction.isdigit()):
        return None
    if len(fraction) > 2:
        return None
    cents = int(whole) * 100 + int((fraction + "00")[:2])
    return -cents if negative else cents


def grid_cell(
    longitude_nano: int | None,
    latitude_nano: int | None,
    *,
    size: int,
    longitude_step: int,
    latitude_step: int,
) -> int | None:
    if longitude_nano is None or latitude_nano is None:
        return None
    west_edge = CENTER_LONGITUDE - longitude_step // 2
    north_edge = CENTER_LATITUDE + latitude_step // 2
    east_offset = longitude_nano - west_edge
    south_offset = north_edge - latitude_nano
    if east_offset < 0 or south_offset < 0:
        return None
    east_index = east_offset // longitude_step + 1
    south_index = south_offset // latitude_step + 1
    if east_index > size or south_index > size:
        return None
    return (east_index << CELL_SHIFT) | south_index


def q1_cell(longitude: str, latitude: str) -> int | None:
    return grid_cell(
        parse_decimal_nano(longitude),
        parse_decimal_nano(latitude),
        size=300,
        longitude_step=Q1_LONGITUDE_STEP,
        latitude_step=Q1_LATITUDE_STEP,
    )


def q2_cell(longitude: str, latitude: str) -> int | None:
    return grid_cell(
        parse_decimal_nano(longitude),
        parse_decimal_nano(latitude),
        size=600,
        longitude_step=Q2_LONGITUDE_STEP,
        latitude_step=Q2_LATITUDE_STEP,
    )


def cell_text(cell: int) -> str:
    return f"{cell >> CELL_SHIFT}.{cell & CELL_MASK}"


def route_id(pickup_cell: int, dropoff_cell: int) -> int:
    return (pickup_cell << ROUTE_SHIFT) | dropoff_cell


def route_cells(value: int) -> tuple[int, int]:
    return value >> ROUTE_SHIFT, value & ((1 << ROUTE_SHIFT) - 1)


@lru_cache(maxsize=512)
def _day_epoch(day: str) -> int:
    parsed = datetime.strptime(day, "%Y-%m-%d")
    return calendar.timegm(parsed.timetuple())


def parse_event_time(value: str) -> int:
    if len(value) != 20 or value[10] != "T" or value[-1] != "Z":
        raise ValueError(f"event time is not canonical UTC: {value!r}")
    return (
        _day_epoch(value[:10])
        + int(value[11:13]) * 3600
        + int(value[14:16]) * 60
        + int(value[17:19])
    )


@lru_cache(maxsize=131_072)
def event_time_text(epoch_seconds: int) -> str:
    return datetime.fromtimestamp(epoch_seconds, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def round_half_even_ratio(numerator: int, denominator: int, places: int = 6) -> str:
    if denominator <= 0 or numerator < 0:
        raise ValueError("ratio must be non-negative with a positive denominator")
    scale = 10**places
    quotient, remainder = divmod(numerator * scale, denominator)
    doubled = remainder * 2
    if doubled > denominator or (doubled == denominator and quotient % 2 == 1):
        quotient += 1
    whole, fraction = divmod(quotient, scale)
    return f"{whole}.{fraction:0{places}d}"


class PreparedEventReader:
    def __init__(self, path: Path, dataset_id: str) -> None:
        self.path = path
        self.dataset_id = dataset_id

    def __iter__(self) -> Iterator[Event]:
        import zstandard

        with self.path.open("rb") as raw:
            reader = zstandard.ZstdDecompressor().stream_reader(raw)
            with io.TextIOWrapper(reader, encoding="utf-8", newline="") as text:
                csv_reader = csv.reader(text)
                header = tuple(next(csv_reader))
                if header != PREPARED_COLUMNS:
                    raise ValueError("prepared CSV header does not match schema version 1")
                index = {name: position for position, name in enumerate(header)}
                expected_line = 1
                previous_time: int | None = None
                for row in csv_reader:
                    if len(row) != len(header):
                        raise ValueError(f"prepared row {expected_line} has wrong width")
                    source_line = int(row[index["source_line_number"]])
                    if source_line != expected_line:
                        raise ValueError(
                            f"source line {source_line} found; expected {expected_line}"
                        )
                    if row[index["dataset_id"]] != self.dataset_id:
                        raise ValueError(f"dataset ID changed at source line {source_line}")
                    event_time = parse_event_time(row[index["dropoff_datetime"]])
                    if previous_time is not None and event_time < previous_time:
                        raise ValueError(f"event time moved backward at line {source_line}")

                    pickup_longitude = row[index["pickup_longitude"]]
                    pickup_latitude = row[index["pickup_latitude"]]
                    dropoff_longitude = row[index["dropoff_longitude"]]
                    dropoff_latitude = row[index["dropoff_latitude"]]
                    pickup_lon_nano = parse_decimal_nano(pickup_longitude)
                    pickup_lat_nano = parse_decimal_nano(pickup_latitude)
                    dropoff_lon_nano = parse_decimal_nano(dropoff_longitude)
                    dropoff_lat_nano = parse_decimal_nano(dropoff_latitude)

                    fare = parse_cents(row[index["fare_amount"]])
                    tip = parse_cents(row[index["tip_amount"]])
                    money = (
                        fare + tip
                        if fare is not None
                        and tip is not None
                        and fare >= 0
                        and tip >= 0
                        else None
                    )
                    yield Event(
                        source_line=source_line,
                        event_id=row[index["event_id"]],
                        event_time=event_time,
                        medallion=row[index["medallion"]],
                        pickup_q1=grid_cell(
                            pickup_lon_nano,
                            pickup_lat_nano,
                            size=300,
                            longitude_step=Q1_LONGITUDE_STEP,
                            latitude_step=Q1_LATITUDE_STEP,
                        ),
                        dropoff_q1=grid_cell(
                            dropoff_lon_nano,
                            dropoff_lat_nano,
                            size=300,
                            longitude_step=Q1_LONGITUDE_STEP,
                            latitude_step=Q1_LATITUDE_STEP,
                        ),
                        pickup_q2=grid_cell(
                            pickup_lon_nano,
                            pickup_lat_nano,
                            size=600,
                            longitude_step=Q2_LONGITUDE_STEP,
                            latitude_step=Q2_LATITUDE_STEP,
                        ),
                        dropoff_q2=grid_cell(
                            dropoff_lon_nano,
                            dropoff_lat_nano,
                            size=600,
                            longitude_step=Q2_LONGITUDE_STEP,
                            latitude_step=Q2_LATITUDE_STEP,
                        ),
                        fare_plus_tip_cents=money,
                    )
                    expected_line += 1
                    previous_time = event_time
