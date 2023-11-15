#
# Copyright (C) 2021 The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import logging
import os
from typing import Any, Callable, Dict, Optional, Sequence
from urllib.parse import urlparse

import fsspec
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from deltastore.converters import PandasConverter, PyArrowConverter
from deltastore.helpers import (
    get_deltasharing_filters,
    get_pyarrow_filters,
    parse_deltasharing_profile_url,
)
from deltastore.profile import DeltaSharingProfile
from deltastore.protocols import AddFile, Table
from deltastore.restclient import DeltaSharingRestClient

logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%Y/%m/%d %I:%M:%S %p",
    level=os.getenv("LOGLEVEL", "INFO").upper(),
)


class DeltaSharingReader:
    def __init__(self, restclient: DeltaSharingRestClient):
        self._restclient = restclient

    def load_pandas(
        self,
        table: Table,
        predicates: Optional[Sequence[str]] = None,
        columns: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        version: Optional[int] = None,
    ) -> pd.DataFrame:
        if limit is not None:
            assert (
                isinstance(limit, int) and limit >= 0
            ), "'limit' must be a non-negative int"

        response = self._restclient.list_files_in_table(
            table,
            predicateHints=get_deltasharing_filters(predicates),
            limitHint=limit,
            version=version,
        )

        schema_json = json.loads(response.metadata.schema_string)

        if len(response.add_files) == 0 or limit == 0:
            return PandasConverter.get_empty_table(schema_json)

        converters = PandasConverter.to_converters(schema_json)
        expressions = get_pyarrow_filters(
            predicates, response.metadata.partition_columns
        )
        _dataframes = []

        if limit is None:
            _dataframe = self._load_pandas(
                response.add_files,
                converters,
                expressions,
                columns,
                response.metadata.partition_columns,
                None,
            )
            if len(_dataframe) > 0:
                _dataframes.append(_dataframe)
        else:
            left = limit
            _dataframes = []
            for file in response.add_files:
                _dataframe = self._load_pandas(
                    [file],
                    converters,
                    expressions,
                    columns,
                    response.metadata.partition_columns,
                    left,
                )
                if len(_dataframe) > 0:
                    _dataframes.append(_dataframe)
                    left -= len(_dataframe)
                assert (
                    left >= 0
                ), f"'_load_pandas' returned too many rows. Required: {left}, returned: {len(_dataframe)}"
                if left == 0:
                    break

        if len(_dataframes) == 0:
            return PandasConverter.get_empty_table(schema_json)
        return pd.concat(
            _dataframes,
            axis=0,
            ignore_index=True,
            copy=False,
        )

    def load_arrow(
        self,
        table: Table,
        predicates: Optional[Sequence[str]] = None,
        columns: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        version: Optional[int] = None,
    ) -> pa.Table:
        if limit is not None:
            assert (
                isinstance(limit, int) and limit >= 0
            ), "'limit' must be a non-negative int"

        response = self._restclient.list_files_in_table(
            table,
            predicateHints=get_deltasharing_filters(predicates),
            limitHint=limit,
            version=version,
        )

        schema_json = json.loads(response.metadata.schema_string)

        if len(response.add_files) == 0 or limit == 0:
            return PyArrowConverter.get_empty_table(schema_json)

        converters = PyArrowConverter.to_converters(schema_json)
        expressions = get_pyarrow_filters(
            predicates, response.metadata.partition_columns
        )
        _tables = []

        if limit is None:
            _table = self._load_arrow(
                response.add_files,
                converters,
                expressions,
                columns,
                response.metadata.partition_columns,
                None,
            )
            if len(_table) > 0:
                _tables.append(_table)
        else:
            left = limit
            for file in response.add_files:
                _table = self._load_arrow(
                    [file],
                    converters,
                    expressions,
                    columns,
                    response.metadata.partition_columns,
                    left,
                )
                if len(_table) > 0:
                    _tables.append(_table)
                    left -= len(_table)
                assert (
                    left >= 0
                ), f"'_load_arrow' returned too many rows. Required: {left}, returned: {len(table)}"
                if left == 0:
                    break

        if len(_tables) == 0:
            return PyArrowConverter.get_empty_table(schema_json)
        return pa.concat_tables(_tables)

    def _load_pandas(
        self,
        add_files: [AddFile],
        converters: Dict[str, Callable[[str], Any]],
        filters: Optional[pa.compute.Expression],
        columns: Optional[str],
        partitions: Optional[str],
        limit: Optional[int],
    ) -> pd.DataFrame:
        url = urlparse(add_files[0].url)
        if "storage.googleapis.com" in (url.netloc.lower()):
            import deltastore._yarl_patch

        protocol = url.scheme
        filesystem = fsspec.filesystem(protocol)

        for file in add_files:
            logging.debug(f"AddFile=${file}")

        _dataset = ds.dataset(
            source=list(map(lambda file: file.url, add_files)),
            format="parquet",
            filesystem=filesystem,
        )
        _table = (
            _dataset.to_table(
                filter=filters,
                columns=list(filter(lambda c: c not in partitions, columns))
                if columns
                else None,
            ).slice(length=limit)
            if limit is not None
            else _dataset.to_table(
                filter=filters,
                columns=list(filter(lambda c: c not in partitions, columns))
                if columns
                else None,
            )
        )
        _dataframe = _table.to_pandas(
            date_as_object=True,
            use_threads=False,
            split_blocks=True,
            self_destruct=True,
        )

        for col, converter in converters.items():
            if not columns or col in columns:
                if col not in _dataframe.columns:
                    if col in add_files[0].partition_values:
                        if converter is not None:
                            _dataframe[col] = converter(
                                add_files[0].partition_values[col]
                            )
                        else:
                            raise ValueError(
                                "Cannot partition on binary or complex columns"
                            )
                    else:
                        _dataframe[col] = None

        return _dataframe

    def _load_arrow(
        self,
        add_files: [AddFile],
        converters: Dict[str, Callable[[str], Any]],
        filters: Optional[pa.compute.Expression],
        columns: Optional[str],
        partitions: Optional[str],
        limit: Optional[int],
    ) -> pa.Table:
        url = urlparse(add_files[0].url)
        if "storage.googleapis.com" in (url.netloc.lower()):
            import deltastore._yarl_patch

        protocol = url.scheme
        filesystem = fsspec.filesystem(protocol)

        for file in add_files:
            logging.debug(f"AddFile=${file}")

        _dataset = ds.dataset(
            source=list(map(lambda file: file.url, add_files)),
            format="parquet",
            filesystem=filesystem,
        )
        _table = (
            _dataset.to_table(
                filter=filters,
                columns=list(filter(lambda c: c not in partitions, columns))
                if columns
                else None,
            ).slice(length=limit)
            if limit is not None
            else _dataset.to_table(
                filter=filters,
                columns=list(filter(lambda c: c not in partitions, columns))
                if columns
                else None,
            )
        )

        for col, converter in converters.items():
            if not columns or col in columns:
                if col not in _table.column_names:
                    if col in add_files[0].partition_values:
                        if converter is not None:
                            _table = _table.append_column(
                                col,
                                pa.array(
                                    [converter(add_files[0].partition_values[col])]
                                    * _table.num_rows
                                ),
                            )
                        else:
                            raise ValueError(
                                "Cannot partition on binary or complex columns"
                            )

        return _table


def load_as_pandas(url, predicates=None, columns=None, limit=None, version=None):
    profile_json, share, schema, table = parse_deltasharing_profile_url(url)
    profile = DeltaSharingProfile.read_from_file(profile_json)
    return DeltaSharingReader(restclient=DeltaSharingRestClient(profile)).load_pandas(
        table=Table(name=table, share=share, schema=schema),
        predicates=predicates,
        columns=columns,
        limit=limit,
        version=version,
    )


def load_as_arrow(url, predicates=None, columns=None, limit=None, version=None):
    profile_json, share, schema, table = parse_deltasharing_profile_url(url)
    profile = DeltaSharingProfile.read_from_file(profile_json)
    return DeltaSharingReader(restclient=DeltaSharingRestClient(profile)).load_arrow(
        table=Table(name=table, share=share, schema=schema),
        predicates=predicates,
        columns=columns,
        limit=limit,
        version=version,
    )
