"""Statistics helper."""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
import contextlib
import dataclasses
from datetime import datetime, timedelta
from functools import lru_cache, partial
from itertools import chain, groupby
import json
import logging
from operator import itemgetter
import os
import re
from statistics import mean
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

from sqlalchemy import Select, and_, bindparam, func, lambda_stmt, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import OperationalError, SQLAlchemyError, StatementError
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import literal_column
from sqlalchemy.sql.lambdas import StatementLambdaElement
import voluptuous as vol

from homeassistant.const import ATTR_UNIT_OF_MEASUREMENT
from homeassistant.core import HomeAssistant, callback, valid_entity_id
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.json import JSONEncoder
from homeassistant.helpers.storage import STORAGE_DIR
from homeassistant.helpers.typing import UNDEFINED, UndefinedType
from homeassistant.util import dt as dt_util
from homeassistant.util.unit_conversion import (
    BaseUnitConverter,
    DataRateConverter,
    DistanceConverter,
    ElectricCurrentConverter,
    ElectricPotentialConverter,
    EnergyConverter,
    InformationConverter,
    MassConverter,
    PowerConverter,
    PressureConverter,
    SpeedConverter,
    TemperatureConverter,
    UnitlessRatioConverter,
    VolumeConverter,
)

from .const import (
    DOMAIN,
    EVENT_RECORDER_5MIN_STATISTICS_GENERATED,
    EVENT_RECORDER_HOURLY_STATISTICS_GENERATED,
    SQLITE_MAX_BIND_VARS,
    SupportedDialect,
)
from .db_schema import (
    STATISTICS_TABLES,
    Statistics,
    StatisticsBase,
    StatisticsMeta,
    StatisticsRuns,
    StatisticsShortTerm,
)
from .models import (
    StatisticData,
    StatisticDataTimestamp,
    StatisticMetaData,
    StatisticResult,
    datetime_to_timestamp_or_none,
    process_timestamp,
)
from .util import (
    database_job_retry_wrapper,
    execute,
    execute_stmt_lambda_element,
    get_instance,
    retryable_database_job,
    session_scope,
)

if TYPE_CHECKING:
    from . import Recorder

QUERY_STATISTICS = (
    Statistics.metadata_id,
    Statistics.start_ts,
    Statistics.mean,
    Statistics.min,
    Statistics.max,
    Statistics.last_reset_ts,
    Statistics.state,
    Statistics.sum,
)

QUERY_STATISTICS_SHORT_TERM = (
    StatisticsShortTerm.metadata_id,
    StatisticsShortTerm.start_ts,
    StatisticsShortTerm.mean,
    StatisticsShortTerm.min,
    StatisticsShortTerm.max,
    StatisticsShortTerm.last_reset_ts,
    StatisticsShortTerm.state,
    StatisticsShortTerm.sum,
)

QUERY_STATISTICS_SUMMARY_MEAN = (
    StatisticsShortTerm.metadata_id,
    func.avg(StatisticsShortTerm.mean),
    # https://github.com/sqlalchemy/sqlalchemy/issues/9189
    # pylint: disable-next=not-callable
    func.min(StatisticsShortTerm.min),
    # https://github.com/sqlalchemy/sqlalchemy/issues/9189
    # pylint: disable-next=not-callable
    func.max(StatisticsShortTerm.max),
)

QUERY_STATISTICS_SUMMARY_SUM = (
    StatisticsShortTerm.metadata_id,
    StatisticsShortTerm.start_ts,
    StatisticsShortTerm.last_reset_ts,
    StatisticsShortTerm.state,
    StatisticsShortTerm.sum,
    func.row_number()
    .over(  # type: ignore[no-untyped-call]
        partition_by=StatisticsShortTerm.metadata_id,
        order_by=StatisticsShortTerm.start_ts.desc(),
    )
    .label("rownum"),
)


STATISTIC_UNIT_TO_UNIT_CONVERTER: dict[str | None, type[BaseUnitConverter]] = {
    **{unit: DataRateConverter for unit in DataRateConverter.VALID_UNITS},
    **{unit: DistanceConverter for unit in DistanceConverter.VALID_UNITS},
    **{unit: ElectricCurrentConverter for unit in ElectricCurrentConverter.VALID_UNITS},
    **{
        unit: ElectricPotentialConverter
        for unit in ElectricPotentialConverter.VALID_UNITS
    },
    **{unit: EnergyConverter for unit in EnergyConverter.VALID_UNITS},
    **{unit: InformationConverter for unit in InformationConverter.VALID_UNITS},
    **{unit: MassConverter for unit in MassConverter.VALID_UNITS},
    **{unit: PowerConverter for unit in PowerConverter.VALID_UNITS},
    **{unit: PressureConverter for unit in PressureConverter.VALID_UNITS},
    **{unit: SpeedConverter for unit in SpeedConverter.VALID_UNITS},
    **{unit: TemperatureConverter for unit in TemperatureConverter.VALID_UNITS},
    **{unit: UnitlessRatioConverter for unit in UnitlessRatioConverter.VALID_UNITS},
    **{unit: VolumeConverter for unit in VolumeConverter.VALID_UNITS},
}


_LOGGER = logging.getLogger(__name__)


class BaseStatisticsRow(TypedDict, total=False):
    """A processed row of statistic data."""

    start: float


class StatisticsRow(BaseStatisticsRow, total=False):
    """A processed row of statistic data."""

    end: float
    last_reset: float | None
    state: float | None
    sum: float | None
    min: float | None
    max: float | None
    mean: float | None


def _get_unit_class(unit: str | None) -> str | None:
    """Get corresponding unit class from from the statistics unit."""
    if converter := STATISTIC_UNIT_TO_UNIT_CONVERTER.get(unit):
        return converter.UNIT_CLASS
    return None


def get_display_unit(
    hass: HomeAssistant,
    statistic_id: str,
    statistic_unit: str | None,
) -> str | None:
    """Return the unit which the statistic will be displayed in."""

    if (converter := STATISTIC_UNIT_TO_UNIT_CONVERTER.get(statistic_unit)) is None:
        return statistic_unit

    state_unit: str | None = statistic_unit
    if state := hass.states.get(statistic_id):
        state_unit = state.attributes.get(ATTR_UNIT_OF_MEASUREMENT)

    if state_unit == statistic_unit or state_unit not in converter.VALID_UNITS:
        # Guard against invalid state unit in the DB
        return statistic_unit

    return state_unit


def _get_statistic_to_display_unit_converter(
    statistic_unit: str | None,
    state_unit: str | None,
    requested_units: dict[str, str] | None,
) -> Callable[[float | None], float | None] | None:
    """Prepare a converter from the statistics unit to display unit."""
    if (converter := STATISTIC_UNIT_TO_UNIT_CONVERTER.get(statistic_unit)) is None:
        return None

    display_unit: str | None
    unit_class = converter.UNIT_CLASS
    if requested_units and unit_class in requested_units:
        display_unit = requested_units[unit_class]
    else:
        display_unit = state_unit

    if display_unit not in converter.VALID_UNITS:
        # Guard against invalid state unit in the DB
        return None

    if display_unit == statistic_unit:
        return None

    convert = converter.convert

    def _from_normalized_unit(val: float | None) -> float | None:
        """Return val."""
        if val is None:
            return val
        return convert(val, statistic_unit, display_unit)

    return _from_normalized_unit


def _get_display_to_statistic_unit_converter(
    display_unit: str | None,
    statistic_unit: str | None,
) -> Callable[[float], float]:
    """Prepare a converter from the display unit to the statistics unit."""

    def no_conversion(val: float) -> float:
        """Return val."""
        return val

    if (converter := STATISTIC_UNIT_TO_UNIT_CONVERTER.get(statistic_unit)) is None:
        return no_conversion

    return partial(converter.convert, from_unit=display_unit, to_unit=statistic_unit)


def _get_unit_converter(
    from_unit: str, to_unit: str
) -> Callable[[float | None], float | None]:
    """Prepare a converter from a unit to another unit."""

    def convert_units(
        val: float | None, conv: type[BaseUnitConverter], from_unit: str, to_unit: str
    ) -> float | None:
        """Return converted val."""
        if val is None:
            return val
        return conv.convert(val, from_unit=from_unit, to_unit=to_unit)

    for conv in STATISTIC_UNIT_TO_UNIT_CONVERTER.values():
        if from_unit in conv.VALID_UNITS and to_unit in conv.VALID_UNITS:
            return partial(
                convert_units, conv=conv, from_unit=from_unit, to_unit=to_unit
            )
    raise HomeAssistantError


def can_convert_units(from_unit: str | None, to_unit: str | None) -> bool:
    """Return True if it's possible to convert from from_unit to to_unit."""
    for converter in STATISTIC_UNIT_TO_UNIT_CONVERTER.values():
        if from_unit in converter.VALID_UNITS and to_unit in converter.VALID_UNITS:
            return True
    return False


@dataclasses.dataclass
class PlatformCompiledStatistics:
    """Compiled Statistics from a platform."""

    platform_stats: list[StatisticResult]
    current_metadata: dict[str, tuple[int, StatisticMetaData]]


def split_statistic_id(entity_id: str) -> list[str]:
    """Split a state entity ID into domain and object ID."""
    return entity_id.split(":", 1)


VALID_STATISTIC_ID = re.compile(r"^(?!.+__)(?!_)[\da-z_]+(?<!_):(?!_)[\da-z_]+(?<!_)$")


def valid_statistic_id(statistic_id: str) -> bool:
    """Test if a statistic ID is a valid format.

    Format: <domain>:<statistic> where both are slugs.
    """
    return VALID_STATISTIC_ID.match(statistic_id) is not None


def validate_statistic_id(value: str) -> str:
    """Validate statistic ID."""
    if valid_statistic_id(value):
        return value

    raise vol.Invalid(f"Statistics ID {value} is an invalid statistic ID")


@dataclasses.dataclass
class ValidationIssue:
    """Error or warning message."""

    type: str
    data: dict[str, str | None] | None = None

    def as_dict(self) -> dict:
        """Return dictionary version."""
        return dataclasses.asdict(self)


def get_start_time() -> datetime:
    """Return start time."""
    now = dt_util.utcnow()
    current_period_minutes = now.minute - now.minute % 5
    current_period = now.replace(minute=current_period_minutes, second=0, microsecond=0)
    last_period = current_period - timedelta(minutes=5)
    return last_period


def _find_duplicates(
    session: Session, table: type[StatisticsBase]
) -> tuple[list[int], list[dict]]:
    """Find duplicated statistics."""
    subquery = (
        session.query(
            table.start,
            table.metadata_id,
            literal_column("1").label("is_duplicate"),
        )
        .group_by(table.metadata_id, table.start)
        # https://github.com/sqlalchemy/sqlalchemy/issues/9189
        # pylint: disable-next=not-callable
        .having(func.count() > 1)
        .subquery()
    )
    query = (
        session.query(
            table.id,
            table.metadata_id,
            table.created,
            table.start,
            table.mean,
            table.min,
            table.max,
            table.last_reset,
            table.state,
            table.sum,
        )
        .outerjoin(
            subquery,
            (subquery.c.metadata_id == table.metadata_id)
            & (subquery.c.start == table.start),
        )
        .filter(subquery.c.is_duplicate == 1)
        .order_by(table.metadata_id, table.start, table.id.desc())
        .limit(1000 * SQLITE_MAX_BIND_VARS)
    )
    duplicates = execute(query)
    original_as_dict = {}
    start = None
    metadata_id = None
    duplicate_ids: list[int] = []
    non_identical_duplicates_as_dict: list[dict] = []

    if not duplicates:
        return (duplicate_ids, non_identical_duplicates_as_dict)

    def columns_to_dict(duplicate: Row) -> dict:
        """Convert a SQLAlchemy row to dict."""
        dict_ = {}
        for key in (
            "id",
            "metadata_id",
            "start",
            "created",
            "mean",
            "min",
            "max",
            "last_reset",
            "state",
            "sum",
        ):
            dict_[key] = getattr(duplicate, key)
        return dict_

    def compare_statistic_rows(row1: dict, row2: dict) -> bool:
        """Compare two statistics rows, ignoring id and created."""
        ignore_keys = {"id", "created"}
        keys1 = set(row1).difference(ignore_keys)
        keys2 = set(row2).difference(ignore_keys)
        return keys1 == keys2 and all(row1[k] == row2[k] for k in keys1)

    for duplicate in duplicates:
        if start != duplicate.start or metadata_id != duplicate.metadata_id:
            original_as_dict = columns_to_dict(duplicate)
            start = duplicate.start
            metadata_id = duplicate.metadata_id
            continue
        duplicate_as_dict = columns_to_dict(duplicate)
        duplicate_ids.append(duplicate.id)
        if not compare_statistic_rows(original_as_dict, duplicate_as_dict):
            non_identical_duplicates_as_dict.append(
                {"duplicate": duplicate_as_dict, "original": original_as_dict}
            )

    return (duplicate_ids, non_identical_duplicates_as_dict)


def _delete_duplicates_from_table(
    session: Session, table: type[StatisticsBase]
) -> tuple[int, list[dict]]:
    """Identify and delete duplicated statistics from a specified table."""
    all_non_identical_duplicates: list[dict] = []
    total_deleted_rows = 0
    while True:
        duplicate_ids, non_identical_duplicates = _find_duplicates(session, table)
        if not duplicate_ids:
            break
        all_non_identical_duplicates.extend(non_identical_duplicates)
        for i in range(0, len(duplicate_ids), SQLITE_MAX_BIND_VARS):
            deleted_rows = (
                session.query(table)
                .filter(table.id.in_(duplicate_ids[i : i + SQLITE_MAX_BIND_VARS]))
                .delete(synchronize_session=False)
            )
            total_deleted_rows += deleted_rows
    return (total_deleted_rows, all_non_identical_duplicates)


@database_job_retry_wrapper("delete statistics duplicates", 3)
def delete_statistics_duplicates(
    instance: Recorder, hass: HomeAssistant, session: Session
) -> None:
    """Identify and delete duplicated statistics.

    A backup will be made of duplicated statistics before it is deleted.
    """
    deleted_statistics_rows, non_identical_duplicates = _delete_duplicates_from_table(
        session, Statistics
    )
    if deleted_statistics_rows:
        _LOGGER.info("Deleted %s duplicated statistics rows", deleted_statistics_rows)

    if non_identical_duplicates:
        isotime = dt_util.utcnow().isoformat()
        backup_file_name = f"deleted_statistics.{isotime}.json"
        backup_path = hass.config.path(STORAGE_DIR, backup_file_name)

        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        with open(backup_path, "w", encoding="utf8") as backup_file:
            json.dump(
                non_identical_duplicates,
                backup_file,
                indent=4,
                sort_keys=True,
                cls=JSONEncoder,
            )
        _LOGGER.warning(
            (
                "Deleted %s non identical duplicated %s rows, a backup of the deleted"
                " rows has been saved to %s"
            ),
            len(non_identical_duplicates),
            Statistics.__tablename__,
            backup_path,
        )

    deleted_short_term_statistics_rows, _ = _delete_duplicates_from_table(
        session, StatisticsShortTerm
    )
    if deleted_short_term_statistics_rows:
        _LOGGER.warning(
            "Deleted duplicated short term statistic rows, please report at %s",
            "https://github.com/home-assistant/core/issues?q=is%3Aopen+is%3Aissue+label%3A%22integration%3A+recorder%22",
        )


def _find_statistics_meta_duplicates(session: Session) -> list[int]:
    """Find duplicated statistics_meta."""
    # When querying the database, be careful to only explicitly query for columns
    # which were present in schema version 29. If querying the table, SQLAlchemy
    # will refer to future columns.
    subquery = (
        session.query(
            StatisticsMeta.statistic_id,
            literal_column("1").label("is_duplicate"),
        )
        .group_by(StatisticsMeta.statistic_id)
        # https://github.com/sqlalchemy/sqlalchemy/issues/9189
        # pylint: disable-next=not-callable
        .having(func.count() > 1)
        .subquery()
    )
    query = (
        session.query(StatisticsMeta.statistic_id, StatisticsMeta.id)
        .outerjoin(
            subquery,
            (subquery.c.statistic_id == StatisticsMeta.statistic_id),
        )
        .filter(subquery.c.is_duplicate == 1)
        .order_by(StatisticsMeta.statistic_id, StatisticsMeta.id.desc())
        .limit(1000 * SQLITE_MAX_BIND_VARS)
    )
    duplicates = execute(query)
    statistic_id = None
    duplicate_ids: list[int] = []

    if not duplicates:
        return duplicate_ids

    for duplicate in duplicates:
        if statistic_id != duplicate.statistic_id:
            statistic_id = duplicate.statistic_id
            continue
        duplicate_ids.append(duplicate.id)

    return duplicate_ids


def _delete_statistics_meta_duplicates(session: Session) -> int:
    """Identify and delete duplicated statistics from a specified table."""
    total_deleted_rows = 0
    while True:
        duplicate_ids = _find_statistics_meta_duplicates(session)
        if not duplicate_ids:
            break
        for i in range(0, len(duplicate_ids), SQLITE_MAX_BIND_VARS):
            deleted_rows = (
                session.query(StatisticsMeta)
                .filter(
                    StatisticsMeta.id.in_(duplicate_ids[i : i + SQLITE_MAX_BIND_VARS])
                )
                .delete(synchronize_session=False)
            )
            total_deleted_rows += deleted_rows
    return total_deleted_rows


def delete_statistics_meta_duplicates(instance: Recorder, session: Session) -> None:
    """Identify and delete duplicated statistics_meta.

    This is used when migrating from schema version 28 to schema version 29.
    """
    deleted_statistics_rows = _delete_statistics_meta_duplicates(session)
    if deleted_statistics_rows:
        statistics_meta_manager = instance.statistics_meta_manager
        statistics_meta_manager.reset()
        statistics_meta_manager.load(session)
        _LOGGER.info(
            "Deleted %s duplicated statistics_meta rows", deleted_statistics_rows
        )


def _compile_hourly_statistics_summary_mean_stmt(
    start_time_ts: float, end_time_ts: float
) -> StatementLambdaElement:
    """Generate the summary mean statement for hourly statistics."""
    return lambda_stmt(
        lambda: select(*QUERY_STATISTICS_SUMMARY_MEAN)
        .filter(StatisticsShortTerm.start_ts >= start_time_ts)
        .filter(StatisticsShortTerm.start_ts < end_time_ts)
        .group_by(StatisticsShortTerm.metadata_id)
        .order_by(StatisticsShortTerm.metadata_id)
    )


def _compile_hourly_statistics_last_sum_stmt(
    start_time_ts: float, end_time_ts: float
) -> StatementLambdaElement:
    """Generate the summary mean statement for hourly statistics."""
    return lambda_stmt(
        lambda: select(
            subquery := (
                select(*QUERY_STATISTICS_SUMMARY_SUM)
                .filter(StatisticsShortTerm.start_ts >= start_time_ts)
                .filter(StatisticsShortTerm.start_ts < end_time_ts)
                .subquery()
            )
        )
        .filter(subquery.c.rownum == 1)
        .order_by(subquery.c.metadata_id)
    )


def _compile_hourly_statistics(session: Session, start: datetime) -> None:
    """Compile hourly statistics.

    This will summarize 5-minute statistics for one hour:
    - average, min max is computed by a database query
    - sum is taken from the last 5-minute entry during the hour
    """
    start_time = start.replace(minute=0)
    start_time_ts = start_time.timestamp()
    end_time = start_time + timedelta(hours=1)
    end_time_ts = end_time.timestamp()

    # Compute last hour's average, min, max
    summary: dict[int, StatisticDataTimestamp] = {}
    stmt = _compile_hourly_statistics_summary_mean_stmt(start_time_ts, end_time_ts)
    stats = execute_stmt_lambda_element(session, stmt)

    if stats:
        for stat in stats:
            metadata_id, _mean, _min, _max = stat
            summary[metadata_id] = {
                "start_ts": start_time_ts,
                "mean": _mean,
                "min": _min,
                "max": _max,
            }

    stmt = _compile_hourly_statistics_last_sum_stmt(start_time_ts, end_time_ts)
    # Get last hour's last sum
    stats = execute_stmt_lambda_element(session, stmt)

    if stats:
        for stat in stats:
            metadata_id, start, last_reset_ts, state, _sum, _ = stat
            if metadata_id in summary:
                summary[metadata_id].update(
                    {
                        "last_reset_ts": last_reset_ts,
                        "state": state,
                        "sum": _sum,
                    }
                )
            else:
                summary[metadata_id] = {
                    "start_ts": start_time_ts,
                    "last_reset_ts": last_reset_ts,
                    "state": state,
                    "sum": _sum,
                }

    # Insert compiled hourly statistics in the database
    session.add_all(
        Statistics.from_stats_ts(metadata_id, summary_item)
        for metadata_id, summary_item in summary.items()
    )


@retryable_database_job("compile missing statistics")
def compile_missing_statistics(instance: Recorder) -> bool:
    """Compile missing statistics."""
    now = dt_util.utcnow()
    period_size = 5
    last_period_minutes = now.minute - now.minute % period_size
    last_period = now.replace(minute=last_period_minutes, second=0, microsecond=0)
    start = now - timedelta(days=instance.keep_days)
    start = start.replace(minute=0, second=0, microsecond=0)
    # Commit every 12 hours of data
    commit_interval = 60 / period_size * 12

    with session_scope(
        session=instance.get_session(),
        exception_filter=_filter_unique_constraint_integrity_error(instance),
    ) as session:
        # Find the newest statistics run, if any
        # https://github.com/sqlalchemy/sqlalchemy/issues/9189
        # pylint: disable-next=not-callable
        if last_run := session.query(func.max(StatisticsRuns.start)).scalar():
            start = max(start, process_timestamp(last_run) + timedelta(minutes=5))

        periods_without_commit = 0
        while start < last_period:
            periods_without_commit += 1
            end = start + timedelta(minutes=period_size)
            _LOGGER.debug("Compiling missing statistics for %s-%s", start, end)
            modified_statistic_ids = _compile_statistics(
                instance, session, start, end >= last_period
            )
            if periods_without_commit == commit_interval or modified_statistic_ids:
                session.commit()
                session.expunge_all()
                periods_without_commit = 0
            start = end

    return True


@retryable_database_job("compile statistics")
def compile_statistics(instance: Recorder, start: datetime, fire_events: bool) -> bool:
    """Compile 5-minute statistics for all integrations with a recorder platform.

    The actual calculation is delegated to the platforms.
    """
    # Return if we already have 5-minute statistics for the requested period
    with session_scope(
        session=instance.get_session(),
        exception_filter=_filter_unique_constraint_integrity_error(instance),
    ) as session:
        modified_statistic_ids = _compile_statistics(
            instance, session, start, fire_events
        )

    if modified_statistic_ids:
        # In the rare case that we have modified statistic_ids, we reload the modified
        # statistics meta data into the cache in a fresh session to ensure that the
        # cache is up to date and future calls to get statistics meta data will
        # not have to hit the database again.
        with session_scope(session=instance.get_session(), read_only=True) as session:
            instance.statistics_meta_manager.get_many(session, modified_statistic_ids)

    return True


def _compile_statistics(
    instance: Recorder, session: Session, start: datetime, fire_events: bool
) -> set[str]:
    """Compile 5-minute statistics for all integrations with a recorder platform.

    This is a helper function for compile_statistics and compile_missing_statistics
    that does not retry on database errors since both callers already retry.

    returns a set of modified statistic_ids if any were modified.
    """
    assert start.tzinfo == dt_util.UTC, "start must be in UTC"
    end = start + timedelta(minutes=5)
    statistics_meta_manager = instance.statistics_meta_manager
    modified_statistic_ids: set[str] = set()

    # Return if we already have 5-minute statistics for the requested period
    if session.query(StatisticsRuns).filter_by(start=start).first():
        _LOGGER.debug("Statistics already compiled for %s-%s", start, end)
        return modified_statistic_ids

    _LOGGER.debug("Compiling statistics for %s-%s", start, end)
    platform_stats: list[StatisticResult] = []
    current_metadata: dict[str, tuple[int, StatisticMetaData]] = {}
    # Collect statistics from all platforms implementing support
    for domain, platform in instance.hass.data[DOMAIN].recorder_platforms.items():
        if not hasattr(platform, "compile_statistics"):
            continue
        compiled: PlatformCompiledStatistics = platform.compile_statistics(
            instance.hass, start, end
        )
        _LOGGER.debug(
            "Statistics for %s during %s-%s: %s",
            domain,
            start,
            end,
            compiled.platform_stats,
        )
        platform_stats.extend(compiled.platform_stats)
        current_metadata.update(compiled.current_metadata)

    # Insert collected statistics in the database
    for stats in platform_stats:
        modified_statistic_id, metadata_id = statistics_meta_manager.update_or_add(
            session, stats["meta"], current_metadata
        )
        if modified_statistic_id is not None:
            modified_statistic_ids.add(modified_statistic_id)
        _insert_statistics(
            session,
            StatisticsShortTerm,
            metadata_id,
            stats["stat"],
        )

    if start.minute == 55:
        # A full hour is ready, summarize it
        _compile_hourly_statistics(session, start)

    session.add(StatisticsRuns(start=start))

    if fire_events:
        instance.hass.bus.fire(EVENT_RECORDER_5MIN_STATISTICS_GENERATED)
        if start.minute == 55:
            instance.hass.bus.fire(EVENT_RECORDER_HOURLY_STATISTICS_GENERATED)

    return modified_statistic_ids


def _adjust_sum_statistics(
    session: Session,
    table: type[StatisticsBase],
    metadata_id: int,
    start_time: datetime,
    adj: float,
) -> None:
    """Adjust statistics in the database."""
    start_time_ts = start_time.timestamp()
    try:
        session.query(table).filter_by(metadata_id=metadata_id).filter(
            table.start_ts >= start_time_ts
        ).update(
            {
                table.sum: table.sum + adj,
            },
            synchronize_session=False,
        )
    except SQLAlchemyError:
        _LOGGER.exception(
            "Unexpected exception when updating statistics %s",
            id,
        )


def _insert_statistics(
    session: Session,
    table: type[StatisticsBase],
    metadata_id: int,
    statistic: StatisticData,
) -> None:
    """Insert statistics in the database."""
    try:
        session.add(table.from_stats(metadata_id, statistic))
    except SQLAlchemyError:
        _LOGGER.exception(
            "Unexpected exception when inserting statistics %s:%s ",
            metadata_id,
            statistic,
        )


def _update_statistics(
    session: Session,
    table: type[StatisticsBase],
    stat_id: int,
    statistic: StatisticData,
) -> None:
    """Insert statistics in the database."""
    try:
        session.query(table).filter_by(id=stat_id).update(
            {
                table.mean: statistic.get("mean"),
                table.min: statistic.get("min"),
                table.max: statistic.get("max"),
                table.last_reset_ts: datetime_to_timestamp_or_none(
                    statistic.get("last_reset")
                ),
                table.state: statistic.get("state"),
                table.sum: statistic.get("sum"),
            },
            synchronize_session=False,
        )
    except SQLAlchemyError:
        _LOGGER.exception(
            "Unexpected exception when updating statistics %s:%s ",
            stat_id,
            statistic,
        )


def get_metadata_with_session(
    instance: Recorder,
    session: Session,
    *,
    statistic_ids: set[str] | None = None,
    statistic_type: Literal["mean"] | Literal["sum"] | None = None,
    statistic_source: str | None = None,
) -> dict[str, tuple[int, StatisticMetaData]]:
    """Fetch meta data.

    Returns a dict of (metadata_id, StatisticMetaData) tuples indexed by statistic_id.
    If statistic_ids is given, fetch metadata only for the listed statistics_ids.
    If statistic_type is given, fetch metadata only for statistic_ids supporting it.
    """
    return instance.statistics_meta_manager.get_many(
        session,
        statistic_ids=statistic_ids,
        statistic_type=statistic_type,
        statistic_source=statistic_source,
    )


def get_metadata(
    hass: HomeAssistant,
    *,
    statistic_ids: set[str] | None = None,
    statistic_type: Literal["mean"] | Literal["sum"] | None = None,
    statistic_source: str | None = None,
) -> dict[str, tuple[int, StatisticMetaData]]:
    """Return metadata for statistic_ids."""
    with session_scope(hass=hass, read_only=True) as session:
        return get_metadata_with_session(
            get_instance(hass),
            session,
            statistic_ids=statistic_ids,
            statistic_type=statistic_type,
            statistic_source=statistic_source,
        )


def clear_statistics(instance: Recorder, statistic_ids: list[str]) -> None:
    """Clear statistics for a list of statistic_ids."""
    with session_scope(session=instance.get_session()) as session:
        instance.statistics_meta_manager.delete(session, statistic_ids)


def update_statistics_metadata(
    instance: Recorder,
    statistic_id: str,
    new_statistic_id: str | None | UndefinedType,
    new_unit_of_measurement: str | None | UndefinedType,
) -> None:
    """Update statistics metadata for a statistic_id."""
    statistics_meta_manager = instance.statistics_meta_manager
    if new_unit_of_measurement is not UNDEFINED:
        with session_scope(session=instance.get_session()) as session:
            statistics_meta_manager.update_unit_of_measurement(
                session, statistic_id, new_unit_of_measurement
            )
    if new_statistic_id is not UNDEFINED and new_statistic_id is not None:
        with session_scope(
            session=instance.get_session(),
            exception_filter=_filter_unique_constraint_integrity_error(instance),
        ) as session:
            statistics_meta_manager.update_statistic_id(
                session, DOMAIN, statistic_id, new_statistic_id
            )


async def async_list_statistic_ids(
    hass: HomeAssistant,
    statistic_ids: set[str] | None = None,
    statistic_type: Literal["mean"] | Literal["sum"] | None = None,
) -> list[dict]:
    """Return all statistic_ids (or filtered one) and unit of measurement.

    Queries the database for existing statistic_ids, as well as integrations with
    a recorder platform for statistic_ids which will be added in the next statistics
    period.
    """
    instance = get_instance(hass)

    if statistic_ids is not None:
        # Try to get the results from the cache since there is nearly
        # always a cache hit.
        statistics_meta_manager = instance.statistics_meta_manager
        metadata = statistics_meta_manager.get_from_cache_threadsafe(statistic_ids)
        if not statistic_ids.difference(metadata):
            result = _statistic_by_id_from_metadata(hass, metadata)
            return _flatten_list_statistic_ids_metadata_result(result)

    return await instance.async_add_executor_job(
        list_statistic_ids,
        hass,
        statistic_ids,
        statistic_type,
    )


def _statistic_by_id_from_metadata(
    hass: HomeAssistant,
    metadata: dict[str, tuple[int, StatisticMetaData]],
) -> dict[str, dict[str, Any]]:
    """Return a list of results for a given metadata dict."""
    return {
        meta["statistic_id"]: {
            "display_unit_of_measurement": get_display_unit(
                hass, meta["statistic_id"], meta["unit_of_measurement"]
            ),
            "has_mean": meta["has_mean"],
            "has_sum": meta["has_sum"],
            "name": meta["name"],
            "source": meta["source"],
            "unit_class": _get_unit_class(meta["unit_of_measurement"]),
            "unit_of_measurement": meta["unit_of_measurement"],
        }
        for _, meta in metadata.values()
    }


def _flatten_list_statistic_ids_metadata_result(
    result: dict[str, dict[str, Any]]
) -> list[dict]:
    """Return a flat dict of metadata."""
    return [
        {
            "statistic_id": _id,
            "display_unit_of_measurement": info["display_unit_of_measurement"],
            "has_mean": info["has_mean"],
            "has_sum": info["has_sum"],
            "name": info.get("name"),
            "source": info["source"],
            "statistics_unit_of_measurement": info["unit_of_measurement"],
            "unit_class": info["unit_class"],
        }
        for _id, info in result.items()
    ]


def list_statistic_ids(
    hass: HomeAssistant,
    statistic_ids: set[str] | None = None,
    statistic_type: Literal["mean"] | Literal["sum"] | None = None,
) -> list[dict]:
    """Return all statistic_ids (or filtered one) and unit of measurement.

    Queries the database for existing statistic_ids, as well as integrations with
    a recorder platform for statistic_ids which will be added in the next statistics
    period.
    """
    result = {}
    instance = get_instance(hass)
    statistics_meta_manager = instance.statistics_meta_manager

    # Query the database
    with session_scope(hass=hass, read_only=True) as session:
        metadata = statistics_meta_manager.get_many(
            session, statistic_type=statistic_type, statistic_ids=statistic_ids
        )
        result = _statistic_by_id_from_metadata(hass, metadata)

    if not statistic_ids or statistic_ids.difference(result):
        # If we want all statistic_ids, or some are missing, we need to query
        # the integrations for the missing ones.
        #
        # Query all integrations with a registered recorder platform
        for platform in hass.data[DOMAIN].recorder_platforms.values():
            if not hasattr(platform, "list_statistic_ids"):
                continue
            platform_statistic_ids = platform.list_statistic_ids(
                hass, statistic_ids=statistic_ids, statistic_type=statistic_type
            )

            for key, meta in platform_statistic_ids.items():
                if key in result:
                    # The database has a higher priority than the integration
                    continue
                result[key] = {
                    "display_unit_of_measurement": meta["unit_of_measurement"],
                    "has_mean": meta["has_mean"],
                    "has_sum": meta["has_sum"],
                    "name": meta["name"],
                    "source": meta["source"],
                    "unit_class": _get_unit_class(meta["unit_of_measurement"]),
                    "unit_of_measurement": meta["unit_of_measurement"],
                }

    # Return a list of statistic_id + metadata
    return _flatten_list_statistic_ids_metadata_result(result)


def _reduce_statistics(
    stats: dict[str, list[StatisticsRow]],
    same_period: Callable[[float, float], bool],
    period_start_end: Callable[[float], tuple[float, float]],
    period: timedelta,
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> dict[str, list[StatisticsRow]]:
    """Reduce hourly statistics to daily or monthly statistics."""
    result: dict[str, list[StatisticsRow]] = defaultdict(list)
    period_seconds = period.total_seconds()
    _want_mean = "mean" in types
    _want_min = "min" in types
    _want_max = "max" in types
    _want_last_reset = "last_reset" in types
    _want_state = "state" in types
    _want_sum = "sum" in types
    for statistic_id, stat_list in stats.items():
        max_values: list[float] = []
        mean_values: list[float] = []
        min_values: list[float] = []
        prev_stat: StatisticsRow = stat_list[0]
        fake_entry: StatisticsRow = {"start": stat_list[-1]["start"] + period_seconds}

        # Loop over the hourly statistics + a fake entry to end the period
        for statistic in chain(stat_list, (fake_entry,)):
            if not same_period(prev_stat["start"], statistic["start"]):
                start, end = period_start_end(prev_stat["start"])
                # The previous statistic was the last entry of the period
                row: StatisticsRow = {
                    "start": start,
                    "end": end,
                }
                if _want_mean:
                    row["mean"] = mean(mean_values) if mean_values else None
                if _want_min:
                    row["min"] = min(min_values) if min_values else None
                if _want_max:
                    row["max"] = max(max_values) if max_values else None
                if _want_last_reset:
                    row["last_reset"] = prev_stat.get("last_reset")
                if _want_state:
                    row["state"] = prev_stat.get("state")
                if _want_sum:
                    row["sum"] = prev_stat["sum"]
                result[statistic_id].append(row)

                max_values = []
                mean_values = []
                min_values = []
            if _want_max and (_max := statistic.get("max")) is not None:
                max_values.append(_max)
            if _want_mean and (_mean := statistic.get("mean")) is not None:
                mean_values.append(_mean)
            if _want_min and (_min := statistic.get("min")) is not None:
                min_values.append(_min)
            prev_stat = statistic

    return result


def reduce_day_ts_factory() -> (
    tuple[
        Callable[[float, float], bool],
        Callable[[float], tuple[float, float]],
    ]
):
    """Return functions to match same day and day start end."""
    _boundries: tuple[float, float] = (0, 0)

    # We have to recreate _local_from_timestamp in the closure in case the timezone changes
    _local_from_timestamp = partial(
        datetime.fromtimestamp, tz=dt_util.DEFAULT_TIME_ZONE
    )

    def _same_day_ts(time1: float, time2: float) -> bool:
        """Return True if time1 and time2 are in the same date."""
        nonlocal _boundries
        if not _boundries[0] <= time1 < _boundries[1]:
            _boundries = _day_start_end_ts_cached(time1)
        return _boundries[0] <= time2 < _boundries[1]

    def _day_start_end_ts(time: float) -> tuple[float, float]:
        """Return the start and end of the period (day) time is within."""
        start_local = _local_from_timestamp(time).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return (
            start_local.astimezone(dt_util.UTC).timestamp(),
            (start_local + timedelta(days=1)).astimezone(dt_util.UTC).timestamp(),
        )

    # We create _day_start_end_ts_cached in the closure in case the timezone changes
    _day_start_end_ts_cached = lru_cache(maxsize=6)(_day_start_end_ts)

    return _same_day_ts, _day_start_end_ts_cached


def _reduce_statistics_per_day(
    stats: dict[str, list[StatisticsRow]],
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> dict[str, list[StatisticsRow]]:
    """Reduce hourly statistics to daily statistics."""
    _same_day_ts, _day_start_end_ts = reduce_day_ts_factory()
    return _reduce_statistics(
        stats, _same_day_ts, _day_start_end_ts, timedelta(days=1), types
    )


def reduce_week_ts_factory() -> (
    tuple[
        Callable[[float, float], bool],
        Callable[[float], tuple[float, float]],
    ]
):
    """Return functions to match same week and week start end."""
    _boundries: tuple[float, float] = (0, 0)

    # We have to recreate _local_from_timestamp in the closure in case the timezone changes
    _local_from_timestamp = partial(
        datetime.fromtimestamp, tz=dt_util.DEFAULT_TIME_ZONE
    )

    def _same_week_ts(time1: float, time2: float) -> bool:
        """Return True if time1 and time2 are in the same year and week."""
        nonlocal _boundries
        if not _boundries[0] <= time1 < _boundries[1]:
            _boundries = _week_start_end_ts_cached(time1)
        return _boundries[0] <= time2 < _boundries[1]

    def _week_start_end_ts(time: float) -> tuple[float, float]:
        """Return the start and end of the period (week) time is within."""
        nonlocal _boundries
        time_local = _local_from_timestamp(time)
        start_local = time_local.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=time_local.weekday())
        return (
            start_local.astimezone(dt_util.UTC).timestamp(),
            (start_local + timedelta(days=7)).astimezone(dt_util.UTC).timestamp(),
        )

    # We create _week_start_end_ts_cached in the closure in case the timezone changes
    _week_start_end_ts_cached = lru_cache(maxsize=6)(_week_start_end_ts)

    return _same_week_ts, _week_start_end_ts_cached


def _reduce_statistics_per_week(
    stats: dict[str, list[StatisticsRow]],
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> dict[str, list[StatisticsRow]]:
    """Reduce hourly statistics to weekly statistics."""
    _same_week_ts, _week_start_end_ts = reduce_week_ts_factory()
    return _reduce_statistics(
        stats, _same_week_ts, _week_start_end_ts, timedelta(days=7), types
    )


def reduce_month_ts_factory() -> (
    tuple[
        Callable[[float, float], bool],
        Callable[[float], tuple[float, float]],
    ]
):
    """Return functions to match same month and month start end."""
    _boundries: tuple[float, float] = (0, 0)

    # We have to recreate _local_from_timestamp in the closure in case the timezone changes
    _local_from_timestamp = partial(
        datetime.fromtimestamp, tz=dt_util.DEFAULT_TIME_ZONE
    )

    def _same_month_ts(time1: float, time2: float) -> bool:
        """Return True if time1 and time2 are in the same year and month."""
        nonlocal _boundries
        if not _boundries[0] <= time1 < _boundries[1]:
            _boundries = _month_start_end_ts_cached(time1)
        return _boundries[0] <= time2 < _boundries[1]

    def _month_start_end_ts(time: float) -> tuple[float, float]:
        """Return the start and end of the period (month) time is within."""
        start_local = _local_from_timestamp(time).replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        # We add 4 days to the end to make sure we are in the next month
        end_local = (start_local.replace(day=28) + timedelta(days=4)).replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        return (
            start_local.astimezone(dt_util.UTC).timestamp(),
            end_local.astimezone(dt_util.UTC).timestamp(),
        )

    # We create _month_start_end_ts_cached in the closure in case the timezone changes
    _month_start_end_ts_cached = lru_cache(maxsize=6)(_month_start_end_ts)

    return _same_month_ts, _month_start_end_ts_cached


def _reduce_statistics_per_month(
    stats: dict[str, list[StatisticsRow]],
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> dict[str, list[StatisticsRow]]:
    """Reduce hourly statistics to monthly statistics."""
    _same_month_ts, _month_start_end_ts = reduce_month_ts_factory()
    return _reduce_statistics(
        stats, _same_month_ts, _month_start_end_ts, timedelta(days=31), types
    )


def _generate_statistics_during_period_stmt(
    columns: Select,
    start_time: datetime,
    end_time: datetime | None,
    metadata_ids: list[int] | None,
    table: type[StatisticsBase],
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> StatementLambdaElement:
    """Prepare a database query for statistics during a given period.

    This prepares a lambda_stmt query, so we don't insert the parameters yet.
    """
    start_time_ts = start_time.timestamp()
    stmt = lambda_stmt(lambda: columns.filter(table.start_ts >= start_time_ts))
    if end_time is not None:
        end_time_ts = end_time.timestamp()
        stmt += lambda q: q.filter(table.start_ts < end_time_ts)
    if metadata_ids:
        stmt += lambda q: q.filter(
            # https://github.com/python/mypy/issues/2608
            table.metadata_id.in_(metadata_ids)  # type:ignore[arg-type]
        )
    stmt += lambda q: q.order_by(table.metadata_id, table.start_ts)
    return stmt


def _generate_max_mean_min_statistic_in_sub_period_stmt(
    columns: Select,
    start_time: datetime | None,
    end_time: datetime | None,
    table: type[StatisticsBase],
    metadata_id: int,
) -> StatementLambdaElement:
    stmt = lambda_stmt(lambda: columns.filter(table.metadata_id == metadata_id))
    if start_time is not None:
        start_time_ts = start_time.timestamp()
        stmt += lambda q: q.filter(table.start_ts >= start_time_ts)
    if end_time is not None:
        end_time_ts = end_time.timestamp()
        stmt += lambda q: q.filter(table.start_ts < end_time_ts)
    return stmt


def _get_max_mean_min_statistic_in_sub_period(
    session: Session,
    result: dict[str, float],
    start_time: datetime | None,
    end_time: datetime | None,
    table: type[StatisticsBase],
    types: set[Literal["max", "mean", "min", "change"]],
    metadata_id: int,
) -> None:
    """Return max, mean and min during the period."""
    # Calculate max, mean, min
    columns = select()
    if "max" in types:
        # https://github.com/sqlalchemy/sqlalchemy/issues/9189
        # pylint: disable-next=not-callable
        columns = columns.add_columns(func.max(table.max))
    if "mean" in types:
        columns = columns.add_columns(func.avg(table.mean))
        # https://github.com/sqlalchemy/sqlalchemy/issues/9189
        # pylint: disable-next=not-callable
        columns = columns.add_columns(func.count(table.mean))
    if "min" in types:
        # https://github.com/sqlalchemy/sqlalchemy/issues/9189
        # pylint: disable-next=not-callable
        columns = columns.add_columns(func.min(table.min))
    stmt = _generate_max_mean_min_statistic_in_sub_period_stmt(
        columns, start_time, end_time, table, metadata_id
    )
    stats = cast(Sequence[Row[Any]], execute_stmt_lambda_element(session, stmt))
    if not stats:
        return
    if "max" in types and (new_max := stats[0].max) is not None:
        old_max = result.get("max")
        result["max"] = max(new_max, old_max) if old_max is not None else new_max
    if "mean" in types and stats[0].avg is not None:
        # https://github.com/sqlalchemy/sqlalchemy/issues/9127
        duration = stats[0].count * table.duration.total_seconds()  # type: ignore[operator]
        result["duration"] = result.get("duration", 0.0) + duration
        result["mean_acc"] = result.get("mean_acc", 0.0) + stats[0].avg * duration
    if "min" in types and (new_min := stats[0].min) is not None:
        old_min = result.get("min")
        result["min"] = min(new_min, old_min) if old_min is not None else new_min


def _get_max_mean_min_statistic(
    session: Session,
    head_start_time: datetime | None,
    head_end_time: datetime | None,
    main_start_time: datetime | None,
    main_end_time: datetime | None,
    tail_start_time: datetime | None,
    tail_end_time: datetime | None,
    tail_only: bool,
    metadata_id: int,
    types: set[Literal["max", "mean", "min", "change"]],
) -> dict[str, float | None]:
    """Return max, mean and min during the period.

    The mean is a time weighted average, combining hourly and 5-minute statistics if
    necessary.
    """
    max_mean_min: dict[str, float] = {}
    result: dict[str, float | None] = {}

    if tail_start_time is not None:
        # Calculate max, mean, min
        _get_max_mean_min_statistic_in_sub_period(
            session,
            max_mean_min,
            tail_start_time,
            tail_end_time,
            StatisticsShortTerm,
            types,
            metadata_id,
        )

    if not tail_only:
        _get_max_mean_min_statistic_in_sub_period(
            session,
            max_mean_min,
            main_start_time,
            main_end_time,
            Statistics,
            types,
            metadata_id,
        )

    if head_start_time is not None:
        _get_max_mean_min_statistic_in_sub_period(
            session,
            max_mean_min,
            head_start_time,
            head_end_time,
            StatisticsShortTerm,
            types,
            metadata_id,
        )

    if "max" in types:
        result["max"] = max_mean_min.get("max")
    if "mean" in types:
        if "mean_acc" not in max_mean_min:
            result["mean"] = None
        else:
            result["mean"] = max_mean_min["mean_acc"] / max_mean_min["duration"]
    if "min" in types:
        result["min"] = max_mean_min.get("min")
    return result


def _first_statistic(
    session: Session,
    table: type[StatisticsBase],
    metadata_id: int,
) -> datetime | None:
    """Return the data of the oldest statistic row for a given metadata id."""
    stmt = lambda_stmt(
        lambda: select(table.start_ts)
        .filter(table.metadata_id == metadata_id)
        .order_by(table.start_ts.asc())
        .limit(1)
    )
    if stats := cast(Sequence[Row], execute_stmt_lambda_element(session, stmt)):
        return dt_util.utc_from_timestamp(stats[0].start_ts)
    return None


def _get_oldest_sum_statistic(
    session: Session,
    head_start_time: datetime | None,
    main_start_time: datetime | None,
    tail_start_time: datetime | None,
    oldest_stat: datetime | None,
    tail_only: bool,
    metadata_id: int,
) -> float | None:
    """Return the oldest non-NULL sum during the period."""

    def _get_oldest_sum_statistic_in_sub_period(
        session: Session,
        start_time: datetime | None,
        table: type[StatisticsBase],
        metadata_id: int,
    ) -> float | None:
        """Return the oldest non-NULL sum during the period."""
        stmt = lambda_stmt(
            lambda: select(table.sum)
            .filter(table.metadata_id == metadata_id)
            .filter(table.sum.is_not(None))
            .order_by(table.start_ts.asc())
            .limit(1)
        )
        if start_time is not None:
            start_time = start_time + table.duration - timedelta.resolution
            if table == StatisticsShortTerm:
                minutes = start_time.minute - start_time.minute % 5
                period = start_time.replace(minute=minutes, second=0, microsecond=0)
            else:
                period = start_time.replace(minute=0, second=0, microsecond=0)
            prev_period = period - table.duration
            prev_period_ts = prev_period.timestamp()
            stmt += lambda q: q.filter(table.start_ts >= prev_period_ts)
        stats = cast(Sequence[Row], execute_stmt_lambda_element(session, stmt))
        return stats[0].sum if stats else None

    oldest_sum: float | None = None

    # This function won't be called if tail_only is False and main_start_time is None
    # the extra checks are added to satisfy MyPy
    if not tail_only and main_start_time is not None and oldest_stat is not None:
        period = main_start_time.replace(minute=0, second=0, microsecond=0)
        prev_period = period - Statistics.duration
        if prev_period < oldest_stat:
            return 0

    if (
        head_start_time is not None
        and (
            oldest_sum := _get_oldest_sum_statistic_in_sub_period(
                session, head_start_time, StatisticsShortTerm, metadata_id
            )
        )
        is not None
    ):
        return oldest_sum

    if not tail_only:
        if (
            oldest_sum := _get_oldest_sum_statistic_in_sub_period(
                session, main_start_time, Statistics, metadata_id
            )
        ) is not None:
            return oldest_sum
        return 0

    if (
        tail_start_time is not None
        and (
            oldest_sum := _get_oldest_sum_statistic_in_sub_period(
                session, tail_start_time, StatisticsShortTerm, metadata_id
            )
        )
    ) is not None:
        return oldest_sum

    return 0


def _get_newest_sum_statistic(
    session: Session,
    head_start_time: datetime | None,
    head_end_time: datetime | None,
    main_start_time: datetime | None,
    main_end_time: datetime | None,
    tail_start_time: datetime | None,
    tail_end_time: datetime | None,
    tail_only: bool,
    metadata_id: int,
) -> float | None:
    """Return the newest non-NULL sum during the period."""

    def _get_newest_sum_statistic_in_sub_period(
        session: Session,
        start_time: datetime | None,
        end_time: datetime | None,
        table: type[StatisticsBase],
        metadata_id: int,
    ) -> float | None:
        """Return the newest non-NULL sum during the period."""
        stmt = lambda_stmt(
            lambda: select(
                table.sum,
            )
            .filter(table.metadata_id == metadata_id)
            .filter(table.sum.is_not(None))
            .order_by(table.start_ts.desc())
            .limit(1)
        )
        if start_time is not None:
            start_time_ts = start_time.timestamp()
            stmt += lambda q: q.filter(table.start_ts >= start_time_ts)
        if end_time is not None:
            end_time_ts = end_time.timestamp()
            stmt += lambda q: q.filter(table.start_ts < end_time_ts)
        stats = cast(Sequence[Row], execute_stmt_lambda_element(session, stmt))

        return stats[0].sum if stats else None

    newest_sum: float | None = None

    if tail_start_time is not None:
        newest_sum = _get_newest_sum_statistic_in_sub_period(
            session, tail_start_time, tail_end_time, StatisticsShortTerm, metadata_id
        )
        if newest_sum is not None:
            return newest_sum

    if not tail_only:
        newest_sum = _get_newest_sum_statistic_in_sub_period(
            session, main_start_time, main_end_time, Statistics, metadata_id
        )
        if newest_sum is not None:
            return newest_sum

    if head_start_time is not None:
        newest_sum = _get_newest_sum_statistic_in_sub_period(
            session, head_start_time, head_end_time, StatisticsShortTerm, metadata_id
        )

    return newest_sum


def statistic_during_period(
    hass: HomeAssistant,
    start_time: datetime | None,
    end_time: datetime | None,
    statistic_id: str,
    types: set[Literal["max", "mean", "min", "change"]] | None,
    units: dict[str, str] | None,
) -> dict[str, Any]:
    """Return a statistic data point for the UTC period start_time - end_time."""
    metadata = None

    if not types:
        types = {"max", "mean", "min", "change"}

    result: dict[str, Any] = {}

    with session_scope(hass=hass, read_only=True) as session:
        # Fetch metadata for the given statistic_id
        if not (
            metadata := get_instance(hass).statistics_meta_manager.get(
                session, statistic_id
            )
        ):
            return result

        metadata_id = metadata[0]

        oldest_stat = _first_statistic(session, Statistics, metadata_id)
        oldest_5_min_stat = None
        if not valid_statistic_id(statistic_id):
            oldest_5_min_stat = _first_statistic(
                session, StatisticsShortTerm, metadata_id
            )

        # To calculate the summary, data from the statistics (hourly) and
        # short_term_statistics (5 minute) tables is combined
        # - The short term statistics table is used for the head and tail of the period,
        #   if the period it doesn't start or end on a full hour
        # - The statistics table is used for the remainder of the time
        now = dt_util.utcnow()
        if end_time is not None and end_time > now:
            end_time = now

        tail_only = (
            start_time is not None
            and end_time is not None
            and end_time - start_time < timedelta(hours=1)
        )

        # Calculate the head period
        head_start_time: datetime | None = None
        head_end_time: datetime | None = None
        if (
            not tail_only
            and oldest_stat is not None
            and oldest_5_min_stat is not None
            and oldest_5_min_stat - oldest_stat < timedelta(hours=1)
            and (start_time is None or start_time < oldest_5_min_stat)
        ):
            # To improve accuracy of averaged for statistics which were added within
            # recorder's retention period.
            head_start_time = oldest_5_min_stat
            head_end_time = oldest_5_min_stat.replace(
                minute=0, second=0, microsecond=0
            ) + timedelta(hours=1)
        elif not tail_only and start_time is not None and start_time.minute:
            head_start_time = start_time
            head_end_time = start_time.replace(
                minute=0, second=0, microsecond=0
            ) + timedelta(hours=1)

        # Calculate the tail period
        tail_start_time: datetime | None = None
        tail_end_time: datetime | None = None
        if end_time is None:
            tail_start_time = now.replace(minute=0, second=0, microsecond=0)
        elif end_time.minute:
            tail_start_time = (
                start_time
                if tail_only
                else end_time.replace(minute=0, second=0, microsecond=0)
            )
            tail_end_time = end_time

        # Calculate the main period
        main_start_time: datetime | None = None
        main_end_time: datetime | None = None
        if not tail_only:
            main_start_time = start_time if head_end_time is None else head_end_time
            main_end_time = end_time if tail_start_time is None else tail_start_time

        if not types.isdisjoint({"max", "mean", "min"}):
            result = _get_max_mean_min_statistic(
                session,
                head_start_time,
                head_end_time,
                main_start_time,
                main_end_time,
                tail_start_time,
                tail_end_time,
                tail_only,
                metadata_id,
                types,
            )

        if "change" in types:
            oldest_sum: float | None
            if start_time is None:
                oldest_sum = 0.0
            else:
                oldest_sum = _get_oldest_sum_statistic(
                    session,
                    head_start_time,
                    main_start_time,
                    tail_start_time,
                    oldest_stat,
                    tail_only,
                    metadata_id,
                )
            newest_sum = _get_newest_sum_statistic(
                session,
                head_start_time,
                head_end_time,
                main_start_time,
                main_end_time,
                tail_start_time,
                tail_end_time,
                tail_only,
                metadata_id,
            )
            # Calculate the difference between the oldest and newest sum
            if oldest_sum is not None and newest_sum is not None:
                result["change"] = newest_sum - oldest_sum
            else:
                result["change"] = None

    state_unit = unit = metadata[1]["unit_of_measurement"]
    if state := hass.states.get(statistic_id):
        state_unit = state.attributes.get(ATTR_UNIT_OF_MEASUREMENT)
    convert = _get_statistic_to_display_unit_converter(unit, state_unit, units)

    return {key: convert(value) if convert else value for key, value in result.items()}


def _statistics_during_period_with_session(
    hass: HomeAssistant,
    session: Session,
    start_time: datetime,
    end_time: datetime | None,
    statistic_ids: set[str] | None,
    period: Literal["5minute", "day", "hour", "week", "month"],
    units: dict[str, str] | None,
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> dict[str, list[StatisticsRow]]:
    """Return statistic data points during UTC period start_time - end_time.

    If end_time is omitted, returns statistics newer than or equal to start_time.
    If statistic_ids is omitted, returns statistics for all statistics ids.
    """
    if statistic_ids is not None and not isinstance(statistic_ids, set):
        # This is for backwards compatibility to avoid a breaking change
        # for custom integrations that call this method.
        statistic_ids = set(statistic_ids)  # type: ignore[unreachable]
    metadata = None
    # Fetch metadata for the given (or all) statistic_ids
    metadata = get_instance(hass).statistics_meta_manager.get_many(
        session, statistic_ids=statistic_ids
    )
    if not metadata:
        return {}

    metadata_ids = None
    if statistic_ids is not None:
        metadata_ids = [metadata_id for metadata_id, _ in metadata.values()]

    table: type[Statistics | StatisticsShortTerm] = (
        Statistics if period != "5minute" else StatisticsShortTerm
    )
    columns = select(table.metadata_id, table.start_ts)  # type: ignore[call-overload]
    if "last_reset" in types:
        columns = columns.add_columns(table.last_reset_ts)
    if "max" in types:
        columns = columns.add_columns(table.max)
    if "mean" in types:
        columns = columns.add_columns(table.mean)
    if "min" in types:
        columns = columns.add_columns(table.min)
    if "state" in types:
        columns = columns.add_columns(table.state)
    if "sum" in types:
        columns = columns.add_columns(table.sum)
    stmt = _generate_statistics_during_period_stmt(
        columns, start_time, end_time, metadata_ids, table, types
    )
    stats = cast(Sequence[Row], execute_stmt_lambda_element(session, stmt))

    if not stats:
        return {}
    # Return statistics combined with metadata
    if period not in ("day", "week", "month"):
        return _sorted_statistics_to_dict(
            hass,
            session,
            stats,
            statistic_ids,
            metadata,
            True,
            table,
            start_time,
            units,
            types,
        )

    result = _sorted_statistics_to_dict(
        hass,
        session,
        stats,
        statistic_ids,
        metadata,
        True,
        table,
        start_time,
        units,
        types,
    )

    if period == "day":
        return _reduce_statistics_per_day(result, types)

    if period == "week":
        return _reduce_statistics_per_week(result, types)

    return _reduce_statistics_per_month(result, types)


def statistics_during_period(
    hass: HomeAssistant,
    start_time: datetime,
    end_time: datetime | None,
    statistic_ids: set[str] | None,
    period: Literal["5minute", "day", "hour", "week", "month"],
    units: dict[str, str] | None,
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> dict[str, list[StatisticsRow]]:
    """Return statistic data points during UTC period start_time - end_time.

    If end_time is omitted, returns statistics newer than or equal to start_time.
    If statistic_ids is omitted, returns statistics for all statistics ids.
    """
    with session_scope(hass=hass, read_only=True) as session:
        return _statistics_during_period_with_session(
            hass,
            session,
            start_time,
            end_time,
            statistic_ids,
            period,
            units,
            types,
        )


def _get_last_statistics_stmt(
    metadata_id: int,
    number_of_stats: int,
) -> StatementLambdaElement:
    """Generate a statement for number_of_stats statistics for a given statistic_id."""
    return lambda_stmt(
        lambda: select(*QUERY_STATISTICS)
        .filter_by(metadata_id=metadata_id)
        .order_by(Statistics.metadata_id, Statistics.start_ts.desc())
        .limit(number_of_stats)
    )


def _get_last_statistics_short_term_stmt(
    metadata_id: int,
    number_of_stats: int,
) -> StatementLambdaElement:
    """Generate a statement for number_of_stats short term statistics.

    For a given statistic_id.
    """
    return lambda_stmt(
        lambda: select(*QUERY_STATISTICS_SHORT_TERM)
        .filter_by(metadata_id=metadata_id)
        .order_by(StatisticsShortTerm.metadata_id, StatisticsShortTerm.start_ts.desc())
        .limit(number_of_stats)
    )


def _get_last_statistics(
    hass: HomeAssistant,
    number_of_stats: int,
    statistic_id: str,
    convert_units: bool,
    table: type[StatisticsBase],
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> dict[str, list[StatisticsRow]]:
    """Return the last number_of_stats statistics for a given statistic_id."""
    statistic_ids = {statistic_id}
    with session_scope(hass=hass, read_only=True) as session:
        # Fetch metadata for the given statistic_id
        metadata = get_instance(hass).statistics_meta_manager.get_many(
            session, statistic_ids=statistic_ids
        )
        if not metadata:
            return {}
        metadata_id = metadata[statistic_id][0]
        if table == Statistics:
            stmt = _get_last_statistics_stmt(metadata_id, number_of_stats)
        else:
            stmt = _get_last_statistics_short_term_stmt(metadata_id, number_of_stats)
        stats = cast(Sequence[Row], execute_stmt_lambda_element(session, stmt))

        if not stats:
            return {}

        # Return statistics combined with metadata
        return _sorted_statistics_to_dict(
            hass,
            session,
            stats,
            statistic_ids,
            metadata,
            convert_units,
            table,
            None,
            None,
            types,
        )


def get_last_statistics(
    hass: HomeAssistant,
    number_of_stats: int,
    statistic_id: str,
    convert_units: bool,
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> dict[str, list[StatisticsRow]]:
    """Return the last number_of_stats statistics for a statistic_id."""
    return _get_last_statistics(
        hass, number_of_stats, statistic_id, convert_units, Statistics, types
    )


def get_last_short_term_statistics(
    hass: HomeAssistant,
    number_of_stats: int,
    statistic_id: str,
    convert_units: bool,
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> dict[str, list[StatisticsRow]]:
    """Return the last number_of_stats short term statistics for a statistic_id."""
    return _get_last_statistics(
        hass, number_of_stats, statistic_id, convert_units, StatisticsShortTerm, types
    )


def _latest_short_term_statistics_stmt(
    metadata_ids: list[int],
) -> StatementLambdaElement:
    """Create the statement for finding the latest short term stat rows."""
    return lambda_stmt(
        lambda: select(*QUERY_STATISTICS_SHORT_TERM).join(
            (
                most_recent_statistic_row := (
                    select(
                        StatisticsShortTerm.metadata_id,
                        # https://github.com/sqlalchemy/sqlalchemy/issues/9189
                        # pylint: disable-next=not-callable
                        func.max(StatisticsShortTerm.start_ts).label("start_max"),
                    )
                    .where(StatisticsShortTerm.metadata_id.in_(metadata_ids))
                    .group_by(StatisticsShortTerm.metadata_id)
                ).subquery()
            ),
            (StatisticsShortTerm.metadata_id == most_recent_statistic_row.c.metadata_id)
            & (StatisticsShortTerm.start_ts == most_recent_statistic_row.c.start_max),
        )
    )


def get_latest_short_term_statistics(
    hass: HomeAssistant,
    statistic_ids: set[str],
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
    metadata: dict[str, tuple[int, StatisticMetaData]] | None = None,
) -> dict[str, list[StatisticsRow]]:
    """Return the latest short term statistics for a list of statistic_ids."""
    with session_scope(hass=hass, read_only=True) as session:
        # Fetch metadata for the given statistic_ids
        if not metadata:
            metadata = get_instance(hass).statistics_meta_manager.get_many(
                session, statistic_ids=statistic_ids
            )
        if not metadata:
            return {}
        metadata_ids = [
            metadata[statistic_id][0]
            for statistic_id in statistic_ids
            if statistic_id in metadata
        ]
        stmt = _latest_short_term_statistics_stmt(metadata_ids)
        stats = cast(Sequence[Row], execute_stmt_lambda_element(session, stmt))
        if not stats:
            return {}

        # Return statistics combined with metadata
        return _sorted_statistics_to_dict(
            hass,
            session,
            stats,
            statistic_ids,
            metadata,
            False,
            StatisticsShortTerm,
            None,
            None,
            types,
        )


def _generate_statistics_at_time_stmt(
    columns: Select,
    table: type[StatisticsBase],
    metadata_ids: set[int],
    start_time_ts: float,
) -> StatementLambdaElement:
    """Create the statement for finding the statistics for a given time."""
    return lambda_stmt(
        lambda: columns.join(
            (
                most_recent_statistic_ids := (
                    select(
                        # https://github.com/sqlalchemy/sqlalchemy/issues/9189
                        # pylint: disable-next=not-callable
                        func.max(table.start_ts).label("max_start_ts"),
                        table.metadata_id.label("max_metadata_id"),
                    )
                    .filter(table.start_ts < start_time_ts)
                    .filter(table.metadata_id.in_(metadata_ids))
                    .group_by(table.metadata_id)
                    .subquery()
                )
            ),
            and_(
                table.start_ts == most_recent_statistic_ids.c.max_start_ts,
                table.metadata_id == most_recent_statistic_ids.c.max_metadata_id,
            ),
        )
    )


def _statistics_at_time(
    session: Session,
    metadata_ids: set[int],
    table: type[StatisticsBase],
    start_time: datetime,
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> Sequence[Row] | None:
    """Return last known statistics, earlier than start_time, for the metadata_ids."""
    columns = select(table.metadata_id, table.start_ts)
    if "last_reset" in types:
        columns = columns.add_columns(table.last_reset_ts)
    if "max" in types:
        columns = columns.add_columns(table.max)
    if "mean" in types:
        columns = columns.add_columns(table.mean)
    if "min" in types:
        columns = columns.add_columns(table.min)
    if "state" in types:
        columns = columns.add_columns(table.state)
    if "sum" in types:
        columns = columns.add_columns(table.sum)
    start_time_ts = start_time.timestamp()
    stmt = _generate_statistics_at_time_stmt(
        columns, table, metadata_ids, start_time_ts
    )
    return cast(Sequence[Row], execute_stmt_lambda_element(session, stmt))


def _sorted_statistics_to_dict(
    hass: HomeAssistant,
    session: Session,
    stats: Sequence[Row[Any]],
    statistic_ids: set[str] | None,
    _metadata: dict[str, tuple[int, StatisticMetaData]],
    convert_units: bool,
    table: type[StatisticsBase],
    start_time: datetime | None,
    units: dict[str, str] | None,
    types: set[Literal["last_reset", "max", "mean", "min", "state", "sum"]],
) -> dict[str, list[StatisticsRow]]:
    """Convert SQL results into JSON friendly data structure."""
    assert stats, "stats must not be empty"  # Guard against implementation error
    result: dict[str, list[StatisticsRow]] = defaultdict(list)
    metadata = dict(_metadata.values())
    need_stat_at_start_time: set[int] = set()
    start_time_ts = start_time.timestamp() if start_time else None
    # Identify metadata IDs for which no data was available at the requested start time
    field_map: dict[str, int] = {key: idx for idx, key in enumerate(stats[0]._fields)}
    metadata_id_idx = field_map["metadata_id"]
    start_ts_idx = field_map["start_ts"]
    stats_by_meta_id: dict[int, list[Row]] = {}
    seen_statistic_ids: set[str] = set()
    key_func = itemgetter(metadata_id_idx)
    for meta_id, group in groupby(stats, key_func):
        stats_list = stats_by_meta_id[meta_id] = list(group)
        seen_statistic_ids.add(metadata[meta_id]["statistic_id"])
        first_start_time_ts = stats_list[0][start_ts_idx]
        if start_time_ts and first_start_time_ts > start_time_ts:
            need_stat_at_start_time.add(meta_id)

    # Set all statistic IDs to empty lists in result set to maintain the order
    if statistic_ids is not None:
        for stat_id in statistic_ids:
            # Only set the statistic ID if it is in the data to
            # avoid having to do a second loop to remove the
            # statistic IDs that are not in the data at the end
            if stat_id in seen_statistic_ids:
                result[stat_id] = []

    # Fetch last known statistics for the needed metadata IDs
    if need_stat_at_start_time:
        assert start_time  # Can not be None if need_stat_at_start_time is not empty
        if tmp := _statistics_at_time(
            session, need_stat_at_start_time, table, start_time, types
        ):
            for stat in tmp:
                stats_by_meta_id[stat[metadata_id_idx]].insert(0, stat)

    # Figure out which fields we need to extract from the SQL result
    # and which indices they have in the result so we can avoid the overhead
    # of doing a dict lookup for each row
    mean_idx = field_map["mean"] if "mean" in types else None
    min_idx = field_map["min"] if "min" in types else None
    max_idx = field_map["max"] if "max" in types else None
    last_reset_ts_idx = field_map["last_reset_ts"] if "last_reset" in types else None
    state_idx = field_map["state"] if "state" in types else None
    sum_idx = field_map["sum"] if "sum" in types else None
    # Append all statistic entries, and optionally do unit conversion
    table_duration_seconds = table.duration.total_seconds()
    for meta_id, stats_list in stats_by_meta_id.items():
        metadata_by_id = metadata[meta_id]
        statistic_id = metadata_by_id["statistic_id"]
        if convert_units:
            state_unit = unit = metadata_by_id["unit_of_measurement"]
            if state := hass.states.get(statistic_id):
                state_unit = state.attributes.get(ATTR_UNIT_OF_MEASUREMENT)
            convert = _get_statistic_to_display_unit_converter(unit, state_unit, units)
        else:
            convert = None
        ent_results_append = result[statistic_id].append
        #
        # The below loop is a red hot path for energy, and every
        # optimization counts in here.
        #
        # Specifically, we want to avoid function calls,
        # attribute lookups, and dict lookups as much as possible.
        #
        for db_state in stats_list:
            row: StatisticsRow = {
                "start": (start_ts := db_state[start_ts_idx]),
                "end": start_ts + table_duration_seconds,
            }
            if last_reset_ts_idx is not None:
                row["last_reset"] = db_state[last_reset_ts_idx]
            if convert:
                if mean_idx is not None:
                    row["mean"] = convert(db_state[mean_idx])
                if min_idx is not None:
                    row["min"] = convert(db_state[min_idx])
                if max_idx is not None:
                    row["max"] = convert(db_state[max_idx])
                if state_idx is not None:
                    row["state"] = convert(db_state[state_idx])
                if sum_idx is not None:
                    row["sum"] = convert(db_state[sum_idx])
            else:
                if mean_idx is not None:
                    row["mean"] = db_state[mean_idx]
                if min_idx is not None:
                    row["min"] = db_state[min_idx]
                if max_idx is not None:
                    row["max"] = db_state[max_idx]
                if state_idx is not None:
                    row["state"] = db_state[state_idx]
                if sum_idx is not None:
                    row["sum"] = db_state[sum_idx]
            ent_results_append(row)

    return result


def validate_statistics(hass: HomeAssistant) -> dict[str, list[ValidationIssue]]:
    """Validate statistics."""
    platform_validation: dict[str, list[ValidationIssue]] = {}
    for platform in hass.data[DOMAIN].recorder_platforms.values():
        if not hasattr(platform, "validate_statistics"):
            continue
        platform_validation.update(platform.validate_statistics(hass))
    return platform_validation


def _statistics_exists(
    session: Session,
    table: type[StatisticsBase],
    metadata_id: int,
    start: datetime,
) -> int | None:
    """Return id if a statistics entry already exists."""
    start_ts = start.timestamp()
    result = (
        session.query(table.id)
        .filter((table.metadata_id == metadata_id) & (table.start_ts == start_ts))
        .first()
    )
    return result.id if result else None


@callback
def _async_import_statistics(
    hass: HomeAssistant,
    metadata: StatisticMetaData,
    statistics: Iterable[StatisticData],
) -> None:
    """Validate timestamps and insert an import_statistics job in the queue."""
    for statistic in statistics:
        start = statistic["start"]
        if start.tzinfo is None or start.tzinfo.utcoffset(start) is None:
            raise HomeAssistantError("Naive timestamp")
        if start.minute != 0 or start.second != 0 or start.microsecond != 0:
            raise HomeAssistantError("Invalid timestamp")
        statistic["start"] = dt_util.as_utc(start)

        if "last_reset" in statistic and statistic["last_reset"] is not None:
            last_reset = statistic["last_reset"]
            if (
                last_reset.tzinfo is None
                or last_reset.tzinfo.utcoffset(last_reset) is None
            ):
                raise HomeAssistantError("Naive timestamp")
            statistic["last_reset"] = dt_util.as_utc(last_reset)

    # Insert job in recorder's queue
    get_instance(hass).async_import_statistics(metadata, statistics, Statistics)


@callback
def async_import_statistics(
    hass: HomeAssistant,
    metadata: StatisticMetaData,
    statistics: Iterable[StatisticData],
) -> None:
    """Import hourly statistics from an internal source.

    This inserts an import_statistics job in the recorder's queue.
    """
    if not valid_entity_id(metadata["statistic_id"]):
        raise HomeAssistantError("Invalid statistic_id")

    # The source must not be empty and must be aligned with the statistic_id
    if not metadata["source"] or metadata["source"] != DOMAIN:
        raise HomeAssistantError("Invalid source")

    _async_import_statistics(hass, metadata, statistics)


@callback
def async_add_external_statistics(
    hass: HomeAssistant,
    metadata: StatisticMetaData,
    statistics: Iterable[StatisticData],
) -> None:
    """Add hourly statistics from an external source.

    This inserts an import_statistics job in the recorder's queue.
    """
    # The statistic_id has same limitations as an entity_id, but with a ':' as separator
    if not valid_statistic_id(metadata["statistic_id"]):
        raise HomeAssistantError("Invalid statistic_id")

    # The source must not be empty and must be aligned with the statistic_id
    domain, _object_id = split_statistic_id(metadata["statistic_id"])
    if not metadata["source"] or metadata["source"] != domain:
        raise HomeAssistantError("Invalid source")

    _async_import_statistics(hass, metadata, statistics)


def _filter_unique_constraint_integrity_error(
    instance: Recorder,
) -> Callable[[Exception], bool]:
    def _filter_unique_constraint_integrity_error(err: Exception) -> bool:
        """Handle unique constraint integrity errors."""
        if not isinstance(err, StatementError):
            return False

        assert instance.engine is not None
        dialect_name = instance.engine.dialect.name

        ignore = False
        if (
            dialect_name == SupportedDialect.SQLITE
            and "UNIQUE constraint failed" in str(err)
        ):
            ignore = True
        if (
            dialect_name == SupportedDialect.POSTGRESQL
            and err.orig
            and hasattr(err.orig, "pgcode")
            and err.orig.pgcode == "23505"
        ):
            ignore = True
        if (
            dialect_name == SupportedDialect.MYSQL
            and err.orig
            and hasattr(err.orig, "args")
        ):
            with contextlib.suppress(TypeError):
                if err.orig.args[0] == 1062:
                    ignore = True

        if ignore:
            _LOGGER.warning(
                (
                    "Blocked attempt to insert duplicated statistic rows, please report"
                    " at %s"
                ),
                "https://github.com/home-assistant/core/issues?q=is%3Aopen+is%3Aissue+label%3A%22integration%3A+recorder%22",
                exc_info=err,
            )

        return ignore

    return _filter_unique_constraint_integrity_error


def _import_statistics_with_session(
    instance: Recorder,
    session: Session,
    metadata: StatisticMetaData,
    statistics: Iterable[StatisticData],
    table: type[StatisticsBase],
) -> bool:
    """Import statistics to the database."""
    statistics_meta_manager = instance.statistics_meta_manager
    old_metadata_dict = statistics_meta_manager.get_many(
        session, statistic_ids={metadata["statistic_id"]}
    )
    _, metadata_id = statistics_meta_manager.update_or_add(
        session, metadata, old_metadata_dict
    )
    for stat in statistics:
        if stat_id := _statistics_exists(session, table, metadata_id, stat["start"]):
            _update_statistics(session, table, stat_id, stat)
        else:
            _insert_statistics(session, table, metadata_id, stat)

    return True


@retryable_database_job("statistics")
def import_statistics(
    instance: Recorder,
    metadata: StatisticMetaData,
    statistics: Iterable[StatisticData],
    table: type[StatisticsBase],
) -> bool:
    """Process an import_statistics job."""

    with session_scope(
        session=instance.get_session(),
        exception_filter=_filter_unique_constraint_integrity_error(instance),
    ) as session:
        return _import_statistics_with_session(
            instance, session, metadata, statistics, table
        )


@retryable_database_job("adjust_statistics")
def adjust_statistics(
    instance: Recorder,
    statistic_id: str,
    start_time: datetime,
    sum_adjustment: float,
    adjustment_unit: str,
) -> bool:
    """Process an add_statistics job."""

    with session_scope(session=instance.get_session()) as session:
        metadata = instance.statistics_meta_manager.get_many(
            session, statistic_ids={statistic_id}
        )
        if statistic_id not in metadata:
            return True

        statistic_unit = metadata[statistic_id][1]["unit_of_measurement"]
        convert = _get_display_to_statistic_unit_converter(
            adjustment_unit, statistic_unit
        )
        sum_adjustment = convert(sum_adjustment)

        _adjust_sum_statistics(
            session,
            StatisticsShortTerm,
            metadata[statistic_id][0],
            start_time,
            sum_adjustment,
        )

        _adjust_sum_statistics(
            session,
            Statistics,
            metadata[statistic_id][0],
            start_time.replace(minute=0),
            sum_adjustment,
        )

    return True


def _change_statistics_unit_for_table(
    session: Session,
    table: type[StatisticsBase],
    metadata_id: int,
    convert: Callable[[float | None], float | None],
) -> None:
    """Insert statistics in the database."""
    columns = (table.id, table.mean, table.min, table.max, table.state, table.sum)
    query = session.query(*columns).filter_by(metadata_id=bindparam("metadata_id"))
    rows = execute(query.params(metadata_id=metadata_id))
    for row in rows:
        session.query(table).filter(table.id == row.id).update(
            {
                table.mean: convert(row.mean),
                table.min: convert(row.min),
                table.max: convert(row.max),
                table.state: convert(row.state),
                table.sum: convert(row.sum),
            },
            synchronize_session=False,
        )


def change_statistics_unit(
    instance: Recorder,
    statistic_id: str,
    new_unit: str,
    old_unit: str,
) -> None:
    """Change statistics unit for a statistic_id."""
    statistics_meta_manager = instance.statistics_meta_manager
    with session_scope(session=instance.get_session()) as session:
        metadata = statistics_meta_manager.get(session, statistic_id)

        # Guard against the statistics being removed or updated before the
        # change_statistics_unit job executes
        if (
            metadata is None
            or metadata[1]["source"] != DOMAIN
            or metadata[1]["unit_of_measurement"] != old_unit
        ):
            _LOGGER.warning("Could not change statistics unit for %s", statistic_id)
            return

        metadata_id = metadata[0]

        convert = _get_unit_converter(old_unit, new_unit)
        tables: tuple[type[StatisticsBase], ...] = (
            Statistics,
            StatisticsShortTerm,
        )
        for table in tables:
            _change_statistics_unit_for_table(session, table, metadata_id, convert)

        statistics_meta_manager.update_unit_of_measurement(
            session, statistic_id, new_unit
        )


@callback
def async_change_statistics_unit(
    hass: HomeAssistant,
    statistic_id: str,
    *,
    new_unit_of_measurement: str,
    old_unit_of_measurement: str,
) -> None:
    """Change statistics unit for a statistic_id."""
    if not can_convert_units(old_unit_of_measurement, new_unit_of_measurement):
        raise HomeAssistantError(
            f"Can't convert {old_unit_of_measurement} to {new_unit_of_measurement}"
        )

    get_instance(hass).async_change_statistics_unit(
        statistic_id,
        new_unit_of_measurement=new_unit_of_measurement,
        old_unit_of_measurement=old_unit_of_measurement,
    )


def _validate_db_schema_utf8(
    instance: Recorder, session_maker: Callable[[], Session]
) -> set[str]:
    """Do some basic checks for common schema errors caused by manual migration."""
    schema_errors: set[str] = set()

    # Lack of full utf8 support is only an issue for MySQL / MariaDB
    if instance.dialect_name != SupportedDialect.MYSQL:
        return schema_errors

    # This name can't be represented unless 4-byte UTF-8 unicode is supported
    utf8_name = "𓆚𓃗"
    statistic_id = f"{DOMAIN}.db_test"

    metadata: StatisticMetaData = {
        "has_mean": True,
        "has_sum": True,
        "name": utf8_name,
        "source": DOMAIN,
        "statistic_id": statistic_id,
        "unit_of_measurement": None,
    }
    statistics_meta_manager = instance.statistics_meta_manager

    # Try inserting some metadata which needs utfmb4 support
    try:
        # Mark the session as read_only to ensure that the test data is not committed
        # to the database and we always rollback when the scope is exited
        with session_scope(session=session_maker(), read_only=True) as session:
            old_metadata_dict = statistics_meta_manager.get_many(
                session, statistic_ids={statistic_id}
            )
            try:
                statistics_meta_manager.update_or_add(
                    session, metadata, old_metadata_dict
                )
                statistics_meta_manager.delete(session, statistic_ids=[statistic_id])
            except OperationalError as err:
                if err.orig and err.orig.args[0] == 1366:
                    _LOGGER.debug(
                        "Database table statistics_meta does not support 4-byte UTF-8"
                    )
                    schema_errors.add("statistics_meta.4-byte UTF-8")
                    session.rollback()
                else:
                    raise
    except Exception as exc:  # pylint: disable=broad-except
        _LOGGER.exception("Error when validating DB schema: %s", exc)
    return schema_errors


def _get_future_year() -> int:
    """Get a year in the future."""
    return datetime.now().year + 1


def _validate_db_schema(
    hass: HomeAssistant, instance: Recorder, session_maker: Callable[[], Session]
) -> set[str]:
    """Do some basic checks for common schema errors caused by manual migration."""
    schema_errors: set[str] = set()
    statistics_meta_manager = instance.statistics_meta_manager

    # Wrong precision is only an issue for MySQL / MariaDB / PostgreSQL
    if instance.dialect_name not in (
        SupportedDialect.MYSQL,
        SupportedDialect.POSTGRESQL,
    ):
        return schema_errors

    # This number can't be accurately represented as a 32-bit float
    precise_number = 1.000000000000001
    # This time can't be accurately represented unless datetimes have µs precision
    #
    # We want to insert statistics for a time in the future, in case they
    # have conflicting metadata_id's with existing statistics that were
    # never cleaned up. By inserting in the future, we can be sure that
    # that by selecting the last inserted row, we will get the one we
    # just inserted.
    #
    future_year = _get_future_year()
    precise_time = datetime(future_year, 10, 6, microsecond=1, tzinfo=dt_util.UTC)
    start_time = datetime(future_year, 10, 6, tzinfo=dt_util.UTC)
    statistic_id = f"{DOMAIN}.db_test"

    metadata: StatisticMetaData = {
        "has_mean": True,
        "has_sum": True,
        "name": None,
        "source": DOMAIN,
        "statistic_id": statistic_id,
        "unit_of_measurement": None,
    }
    statistics: StatisticData = {
        "last_reset": precise_time,
        "max": precise_number,
        "mean": precise_number,
        "min": precise_number,
        "start": precise_time,
        "state": precise_number,
        "sum": precise_number,
    }

    def check_columns(
        schema_errors: set[str],
        stored: Mapping,
        expected: Mapping,
        columns: tuple[str, ...],
        table_name: str,
        supports: str,
    ) -> None:
        for column in columns:
            if stored[column] != expected[column]:
                schema_errors.add(f"{table_name}.{supports}")
                _LOGGER.error(
                    "Column %s in database table %s does not support %s (stored=%s != expected=%s)",
                    column,
                    table_name,
                    supports,
                    stored[column],
                    expected[column],
                )

    # Insert / adjust a test statistics row in each of the tables
    tables: tuple[type[Statistics | StatisticsShortTerm], ...] = (
        Statistics,
        StatisticsShortTerm,
    )
    try:
        # Mark the session as read_only to ensure that the test data is not committed
        # to the database and we always rollback when the scope is exited
        with session_scope(session=session_maker(), read_only=True) as session:
            for table in tables:
                _import_statistics_with_session(
                    instance, session, metadata, (statistics,), table
                )
                stored_statistics = _statistics_during_period_with_session(
                    hass,
                    session,
                    start_time,
                    None,
                    {statistic_id},
                    "hour" if table == Statistics else "5minute",
                    None,
                    {"last_reset", "max", "mean", "min", "state", "sum"},
                )
                if not (stored_statistic := stored_statistics.get(statistic_id)):
                    _LOGGER.warning(
                        "Schema validation failed for table: %s", table.__tablename__
                    )
                    continue

                # We want to look at the last inserted row to make sure there
                # is not previous garbage data in the table that would cause
                # the test to produce an incorrect result. To achieve this,
                # we inserted a row in the future, and now we select the last
                # inserted row back.
                last_stored_statistic = stored_statistic[-1]
                check_columns(
                    schema_errors,
                    last_stored_statistic,
                    statistics,
                    ("max", "mean", "min", "state", "sum"),
                    table.__tablename__,
                    "double precision",
                )
                assert statistics["last_reset"]
                check_columns(
                    schema_errors,
                    last_stored_statistic,
                    {
                        "last_reset": datetime_to_timestamp_or_none(
                            statistics["last_reset"]
                        ),
                        "start": datetime_to_timestamp_or_none(statistics["start"]),
                    },
                    ("start", "last_reset"),
                    table.__tablename__,
                    "µs precision",
                )
            statistics_meta_manager.delete(session, statistic_ids=[statistic_id])
    except Exception as exc:  # pylint: disable=broad-except
        _LOGGER.exception("Error when validating DB schema: %s", exc)

    return schema_errors


def validate_db_schema(
    hass: HomeAssistant, instance: Recorder, session_maker: Callable[[], Session]
) -> set[str]:
    """Do some basic checks for common schema errors caused by manual migration."""
    schema_errors: set[str] = set()
    schema_errors |= _validate_db_schema_utf8(instance, session_maker)
    schema_errors |= _validate_db_schema(hass, instance, session_maker)
    if schema_errors:
        _LOGGER.debug(
            "Detected statistics schema errors: %s", ", ".join(sorted(schema_errors))
        )
    return schema_errors


def correct_db_schema(
    instance: Recorder,
    engine: Engine,
    session_maker: Callable[[], Session],
    schema_errors: set[str],
) -> None:
    """Correct issues detected by validate_db_schema."""
    from .migration import _modify_columns  # pylint: disable=import-outside-toplevel

    if "statistics_meta.4-byte UTF-8" in schema_errors:
        # Attempt to convert the table to utf8mb4
        _LOGGER.warning(
            (
                "Updating character set and collation of table %s to utf8mb4. "
                "Note: this can take several minutes on large databases and slow "
                "computers. Please be patient!"
            ),
            "statistics_meta",
        )
        with contextlib.suppress(SQLAlchemyError), session_scope(
            session=session_maker()
        ) as session:
            connection = session.connection()
            connection.execute(
                # Using LOCK=EXCLUSIVE to prevent the database from corrupting
                # https://github.com/home-assistant/core/issues/56104
                text(
                    "ALTER TABLE statistics_meta CONVERT TO CHARACTER SET utf8mb4"
                    " COLLATE utf8mb4_unicode_ci, LOCK=EXCLUSIVE"
                )
            )

    tables: tuple[type[Statistics | StatisticsShortTerm], ...] = (
        Statistics,
        StatisticsShortTerm,
    )
    for table in tables:
        if f"{table.__tablename__}.double precision" in schema_errors:
            # Attempt to convert float columns to double precision
            _modify_columns(
                session_maker,
                engine,
                table.__tablename__,
                [
                    "mean DOUBLE PRECISION",
                    "min DOUBLE PRECISION",
                    "max DOUBLE PRECISION",
                    "state DOUBLE PRECISION",
                    "sum DOUBLE PRECISION",
                ],
            )
        if f"{table.__tablename__}.µs precision" in schema_errors:
            # Attempt to convert timestamp columns to µs precision
            _modify_columns(
                session_maker,
                engine,
                table.__tablename__,
                [
                    "last_reset_ts DOUBLE PRECISION",
                    "start_ts DOUBLE PRECISION",
                ],
            )


def cleanup_statistics_timestamp_migration(instance: Recorder) -> bool:
    """Clean up the statistics migration from timestamp to datetime.

    Returns False if there are more rows to update.
    Returns True if all rows have been updated.
    """
    engine = instance.engine
    assert engine is not None
    if engine.dialect.name == SupportedDialect.SQLITE:
        for table in STATISTICS_TABLES:
            with session_scope(session=instance.get_session()) as session:
                session.connection().execute(
                    text(
                        f"update {table} set start = NULL, created = NULL, last_reset = NULL;"
                    )
                )
    elif engine.dialect.name == SupportedDialect.MYSQL:
        for table in STATISTICS_TABLES:
            with session_scope(session=instance.get_session()) as session:
                if (
                    session.connection()
                    .execute(
                        text(
                            f"UPDATE {table} set start=NULL, created=NULL, last_reset=NULL where start is not NULL LIMIT 250000;"
                        )
                    )
                    .rowcount
                ):
                    # We have more rows to update so return False
                    # to indicate we need to run again
                    return False
    elif engine.dialect.name == SupportedDialect.POSTGRESQL:
        for table in STATISTICS_TABLES:
            with session_scope(session=instance.get_session()) as session:
                if (
                    session.connection()
                    .execute(
                        text(
                            f"UPDATE {table} set start=NULL, created=NULL, last_reset=NULL "  # nosec
                            f"where id in (select id from {table} where start is not NULL LIMIT 250000)"
                        )
                    )
                    .rowcount
                ):
                    # We have more rows to update so return False
                    # to indicate we need to run again
                    return False

    from .migration import _drop_index  # pylint: disable=import-outside-toplevel

    for table in STATISTICS_TABLES:
        _drop_index(instance.get_session, table, f"ix_{table}_start")
    # We have no more rows to update so return True
    # to indicate we are done
    return True
