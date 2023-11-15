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

import operator
from functools import reduce
from typing import Tuple

import pyarrow.dataset as ds


def parse_deltasharing_profile_url(url: str) -> Tuple[str, str, str, str]:
    """
    :param url: a url under the format "<profile>#<share>.<schema>.<table>"
    :return: a tuple with parsed (profile, share, schema, table)
    """
    shape_index = url.rfind("#")
    if shape_index < 0:
        raise ValueError(f"Invalid 'url': {url}")
    profile = url[0:shape_index]
    fragments = url[shape_index + 1 :].split(".")
    if len(fragments) != 3:
        raise ValueError(f"Invalid 'url': {url}")
    share, schema, table = fragments
    if len(profile) == 0 or len(share) == 0 or len(schema) == 0 or len(table) == 0:
        raise ValueError(f"Invalid 'url': {url}")
    return (profile, share, schema, table)


def get_deltasharing_filters(filters):
    predicates = []
    if filters is not None:
        for conjunction in filters:
            col, op, val = conjunction
            if isinstance(val, str):
                predicates.append(f'{col}{op}"{val}"')
            else:
                predicates.append(f"{col}{op}{val}")
    return predicates


def get_pyarrow_filters(filters, partitions):
    def convert_single_expression(col, op, val):
        field = ds.field(col)

        if op == "=" or op == "==":
            return field == val
        elif op == "!=":
            return field != val
        elif op == "<":
            return field < val
        elif op == ">":
            return field > val
        elif op == "<=":
            return field <= val
        elif op == ">=":
            return field >= val
        elif op == "in":
            return field.isin(val)
        elif op == "not in":
            return ~field.isin(val)
        else:
            raise ValueError(
                '"{0}" is not a valid operator in predicates.'.format((col, op, val))
            )

    expressions = []
    if filters is not None:
        for conjunction in filters:
            col, op, val = conjunction
            if col not in partitions:
                expression = convert_single_expression(col, op, val)
                expressions.append(expression)
    if len(expressions) > 0:
        return reduce(operator.and_, expressions)
