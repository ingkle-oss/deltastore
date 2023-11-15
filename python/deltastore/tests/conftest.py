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

import os
import subprocess
import threading
from pathlib import Path
from typing import Iterator, Optional

import pytest
from deltastore.profile import DeltaSharingProfile
from deltastore.restclient import DeltaSharingRestClient
from pytest import TempPathFactory


@pytest.fixture
def profile_path() -> str:
    return os.path.join(os.path.dirname(__file__), "profile.json")


@pytest.fixture
def share_name() -> str:
    return "deltastore"


@pytest.fixture
def schema_name() -> str:
    return "testsets"


@pytest.fixture
def simple_table_name() -> str:
    return "testset100"


@pytest.fixture
def multilogs_table_name() -> str:
    return "testset100-multilogs"


@pytest.fixture
def profile(profile_path) -> DeltaSharingProfile:
    return DeltaSharingProfile.read_from_file(profile_path)


@pytest.fixture
def restclient(profile) -> DeltaSharingRestClient:
    return DeltaSharingRestClient(profile)


@pytest.fixture(scope="session", autouse=True)
def test_server(tmp_path_factory: TempPathFactory) -> Iterator[None]:
    pidfile: Optional[Path] = None
    proc: Optional[subprocess.Popen] = None
    try:
        pidfile = tmp_path_factory.getbasetemp() / "deltastoretester.pid"
        proc = subprocess.Popen(
            [
                "sbt",
                " ".join(
                    [
                        "server/test:runMain io.delta.store.DeltaStoreTester",
                        "-c",
                        "../python/deltastore/tests/deltastore.yaml",
                        "-p",
                        str(pidfile),
                        "-r",
                        "240",
                    ]
                ),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        ready = threading.Event()

        def wait_for_server() -> None:
            for line in proc.stdout:
                print(line.decode("utf-8").strip())
                if b"127.0.0.1" in line:
                    ready.set()

        threading.Thread(target=wait_for_server, daemon=True).start()

        if not ready.wait(timeout=120):
            raise TimeoutError("the server didn't start in 120 seconds")
        yield
    finally:
        if pidfile is not None and pidfile.exists():
            pid = pidfile.read_text()
            subprocess.run(["kill", "-9", pid])
        if proc is not None and proc.poll() is None:
            proc.kill()
