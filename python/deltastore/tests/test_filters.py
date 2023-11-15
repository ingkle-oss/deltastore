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

from deltastore.helpers import get_deltasharing_filters
from deltastore.profile import DeltaSharingProfile
from deltastore.protocols import Table
from deltastore.restclient import DeltaSharingRestClient

logger = logging.getLogger("deltastore")


def test_simple_query(
    profile_path: str, share_name: str, schema_name: str, simple_table_name: str
):
    profile = DeltaSharingProfile.read_from_file(profile_path)
    client = DeltaSharingRestClient(profile)

    response = client.list_files_in_table(
        Table(name=simple_table_name, share=share_name, schema=schema_name),
    )
    for add_file in response.add_files:
        logger.info(add_file)

    assert len(response.add_files) == 55


def test_partitions_filter(
    profile_path: str, share_name: str, schema_name: str, simple_table_name: str
):
    predicates = []
    predicates.append(("date", ">", "2023-03-15"))
    logger.info(predicates)

    profile = DeltaSharingProfile.read_from_file(profile_path)
    client = DeltaSharingRestClient(profile)

    response = client.list_files_in_table(
        Table(name=simple_table_name, share=share_name, schema=schema_name),
        predicateHints=get_deltasharing_filters(predicates),
    )
    for add_file in response.add_files:
        logger.info(add_file)

    assert len(response.add_files) == 6


def test_statistics_filter(
    profile_path: str, share_name: str, schema_name: str, simple_table_name: str
):
    predicates = []
    predicates.append(("date", ">", "2023-03-15"))
    predicates.append(("name", "<", "Christopher"))
    logger.info(predicates)

    profile = DeltaSharingProfile.read_from_file(profile_path)
    client = DeltaSharingRestClient(profile)

    response = client.list_files_in_table(
        Table(name=simple_table_name, share=share_name, schema=schema_name),
        predicateHints=get_deltasharing_filters(predicates),
    )
    for add_file in response.add_files:
        logger.info(add_file)

    assert len(response.add_files) == 2
