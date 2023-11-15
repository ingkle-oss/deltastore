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

import logging

from deltastore.reader import load_as_pandas

logger = logging.getLogger("deltastore")


def test_simple_query(
    profile_path: str, share_name: str, schema_name: str, simple_table_name: str
):
    url = "{}#{}.{}.{}".format(profile_path, share_name, schema_name, simple_table_name)

    df = load_as_pandas(url)
    logger.info(df)

    assert len(df) == 100


def test_predicates_filter(
    profile_path: str, share_name: str, schema_name: str, simple_table_name: str
):
    url = "{}#{}.{}.{}".format(profile_path, share_name, schema_name, simple_table_name)
    predicates = [("date", ">", "2023-03-15")]

    df = load_as_pandas(url, predicates=predicates)
    logger.info(df)

    assert len(df) == 13


def test_predicates_limit(
    profile_path: str, share_name: str, schema_name: str, simple_table_name: str
):
    url = "{}#{}.{}.{}".format(profile_path, share_name, schema_name, simple_table_name)
    predicates = [("date", ">", "2023-03-15")]
    limit = 3

    df = load_as_pandas(url, predicates=predicates, limit=limit)
    logger.info(df)

    assert len(df) == limit


def test_projections_filter(
    profile_path: str, share_name: str, schema_name: str, simple_table_name: str
):
    url = "{}#{}.{}.{}".format(profile_path, share_name, schema_name, simple_table_name)
    predicates = [("date", ">", "2023-03-15")]
    columns = ["date", "score"]

    df = load_as_pandas(url, predicates=predicates, columns=columns)
    logger.info(df)

    assert len(df.columns) == len(columns)
