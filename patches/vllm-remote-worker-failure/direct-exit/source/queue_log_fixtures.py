# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project
# Exact fixtures from parent tests/conftest.py; no other conftest side effects.
import pytest


@pytest.fixture()
def temporary_enable_log_propagate():
    import logging

    logger = logging.getLogger("vllm")
    previous_propagate = logger.propagate
    logger.propagate = True
    yield
    logger.propagate = previous_propagate


@pytest.fixture()
def caplog_vllm(temporary_enable_log_propagate, caplog):
    # caplog depends on logs propagated to the root logger. The vLLM logger
    # inherits this INFO level until runtime initialization configures it.
    import logging

    with caplog.at_level(logging.INFO):
        yield caplog
