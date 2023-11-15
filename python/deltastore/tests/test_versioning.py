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


def test_simple_query_with_version(
    profile_path: str, share_name: str, schema_name: str, multilogs_table_name: str
):
    predicates = []
    predicates.append(("date", "=", "2023-02-12"))
    logger.info(predicates)

    profile = DeltaSharingProfile.read_from_file(profile_path)
    client = DeltaSharingRestClient(profile)

    response = client.list_files_in_table(
        Table(name=multilogs_table_name, share=share_name, schema=schema_name),
        predicateHints=get_deltasharing_filters(predicates),
        version=8,
    )
    for add_file in response.add_files:
        logger.info(add_file)

    assert len(response.add_files) == 2


def test_remove_actions_with_version(
    profile_path: str, share_name: str, schema_name: str, multilogs_table_name: str
):
    predicates = []
    predicates.append(("date", "=", "2023-02-12"))
    logger.info(predicates)

    profile = DeltaSharingProfile.read_from_file(profile_path)
    client = DeltaSharingRestClient(profile)

    response = client.list_files_in_table(
        Table(name=multilogs_table_name, share=share_name, schema=schema_name),
        predicateHints=get_deltasharing_filters(predicates),
        version=9,
    )
    for add_file in response.add_files:
        logger.info(add_file)

    assert len(response.add_files) == 1
