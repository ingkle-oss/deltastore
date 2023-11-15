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

from dataclasses import dataclass
from json import loads
from pathlib import Path
from typing import IO, ClassVar, Optional, Union

import fsspec


@dataclass(frozen=True)
class DeltaSharingProfile:
    CURRENT: ClassVar[int] = 1

    share_credentials_version: int
    endpoint: str
    bearer_token: str
    expiration_time: Optional[str] = None

    def __post_init__(self):
        if self.share_credentials_version > DeltaSharingProfile.CURRENT:
            raise ValueError(
                "'shareCredentialsVersion' in the profile is "
                f"{self.share_credentials_version} which is too new. "
                f"The current release supports version {DeltaSharingProfile.CURRENT} and below. "
                "Please upgrade to a newer release."
            )

    @staticmethod
    def read_from_file(profile: Union[str, IO, Path]) -> "DeltaSharingProfile":
        if isinstance(profile, str):
            infile = fsspec.open(profile).open()
        elif isinstance(profile, Path):
            infile = fsspec.open(profile.as_uri()).open()
        else:
            infile = profile
        try:
            return DeltaSharingProfile.from_json(infile.read())
        finally:
            infile.close()

    @staticmethod
    def from_json(json) -> "DeltaSharingProfile":
        if isinstance(json, (str, bytes, bytearray)):
            json = loads(json)
        endpoint = json["endpoint"]
        if endpoint.endswith("/"):
            endpoint = endpoint[:-1]
        expiration_time = json.get("expirationTime")
        return DeltaSharingProfile(
            share_credentials_version=int(json["shareCredentialsVersion"]),
            endpoint=endpoint,
            bearer_token=json["bearerToken"],
            expiration_time=expiration_time,
        )
