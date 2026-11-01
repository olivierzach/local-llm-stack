# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project
"""Tests for failing fast when a worker hits a fatal RPC failure."""

import multiprocessing
import os
import struct
import threading
from collections import deque
from types import SimpleNamespace
from typing import Any

import pytest
import zmq

from vllm.distributed.device_communicators.shm_broadcast import MessageQueue
from vllm.v1.executor.multiproc_executor import (
    FutureWrapper,
    MultiprocExecutor,
    WorkerProc,
)


@pytest.fixture
def error_pipe():
    reader, writer = multiprocessing.Pipe(duplex=False)
    yield reader, writer
    reader.close()
    writer.close()


def _make_failing_worker_proc(rank: int, method: str, error_writer) -> Any:
    def fail(*args, **kwargs):
        raise RuntimeError(f"{method} failed")

    worker_proc: Any = WorkerProc.__new__(WorkerProc)
    worker_proc.rank = rank
    worker_proc.worker = SimpleNamespace(**{method: fail})
    worker_proc.error_writer = error_writer
    worker_proc.outputs = []
    worker_proc.handle_output = worker_proc.outputs.append
    return worker_proc


def test_recoverable_rpc_failure_is_only_returned_to_caller(error_pipe):
    """A failed utility RPC, e.g. loading an invalid LoRA adapter, must not
    fail the executor."""
    reader, writer = error_pipe
    worker_proc = _make_failing_worker_proc(0, "add_lora", writer)

    worker_proc._execute_worker_rpc(("add_lora", (), {}, None))

    assert len(worker_proc.outputs) == 1
    assert isinstance(worker_proc.outputs[0], RuntimeError)
    assert not reader.poll(0)


@pytest.mark.parametrize(
    ("method", "output_rank"),
    [("compile_or_warm_up_model", None), ("execute_model", 0)],
)
def test_fatal_rpc_failure_notifies_monitor_once(error_pipe, method, output_rank):
    """A fatal RPC failure must wake up the executor monitor even on a rank
    that doesn't reply, since its peers may be blocked in a collective."""
    reader, writer = error_pipe
    worker_proc = _make_failing_worker_proc(1, method, writer)

    for _ in range(2):
        worker_proc._execute_worker_rpc((method, (), {}, output_rank))

    assert len(worker_proc.outputs) == (2 if output_rank is None else 0)
    assert reader.poll(0)
    reader.recv_bytes()
    assert not reader.poll(0)


def _send_notification(writer) -> None:
    worker_proc: Any = WorkerProc.__new__(WorkerProc)
    worker_proc.error_writer = writer
    worker_proc._report_fatal_failure()


def _write_partial_message(writer) -> None:
    # A worker dying mid-send leaves a torn frame, for which Connection.recv()
    # raises OSError rather than EOFError.
    os.write(writer.fileno(), struct.pack("!i", 1 << 20) + b"partial")
    writer.close()


@pytest.mark.parametrize(
    "signal_failure",
    [_send_notification, _write_partial_message, lambda writer: writer.close()],
    ids=["notification", "partial_message", "closed"],
)
def test_monitor_fails_executor_on_error_pipe_event(error_pipe, signal_failure):
    """Teardown and the failure callback must not depend on what, if anything,
    can be read from the error pipe."""
    reader, writer = error_pipe
    # Never readable while both ends are open, i.e. the worker is alive.
    sentinel_reader, sentinel_writer = os.pipe()
    calls: list[str] = []
    executor: Any = MultiprocExecutor.__new__(MultiprocExecutor)
    executor.is_failed = False
    executor.shutdown = lambda: calls.append("shutdown")
    executor.failure_callback = lambda: calls.append("failure_callback")
    executor.response_mqs = []
    executor.workers = [
        SimpleNamespace(
            proc=SimpleNamespace(
                sentinel=sentinel_reader, name="VllmWorker-1", exitcode=None
            ),
            error_reader=reader,
        )
    ]

    signal_failure(writer)
    monitor = threading.Thread(
        target=executor.start_worker_monitor, kwargs={"inline": True}, daemon=True
    )
    monitor.start()
    monitor.join(timeout=10)
    # Check before closing the sentinel, which would wake up a stuck monitor.
    alive = monitor.is_alive()
    os.close(sentinel_reader)
    os.close(sentinel_writer)

    assert not alive, "monitor did not react to the error pipe"
    assert executor.is_failed
    assert calls == ["shutdown", "failure_callback"]


@pytest.fixture
def remote_queue():
    writer = MessageQueue(1, 0, connect_ip="127.0.0.1")
    reader = MessageQueue.create_from_handle(writer.export_handle(), -1)
    # Arm before the test's monitor thread starts so disconnect delivery does
    # not depend on that thread being scheduled before the publisher closes.
    monitor = reader.remote_socket.get_monitor_socket(events=zmq.EVENT_DISCONNECTED)
    for mq in (writer, reader):
        mq.remote_socket.setsockopt(zmq.RCVTIMEO, 10000)
    writer.wait_until_ready()
    reader.wait_until_ready()
    try:
        yield writer, reader
    finally:
        monitor.close(linger=0)
        for mq in (writer, reader):
            mq.remote_socket.context.destroy(linger=0)


def _wait_for_parent(death_reader, error_writer, ready):
    ready.set()
    try:
        death_reader.recv()
    except EOFError:
        pass
    finally:
        death_reader.close()
        error_writer.close()


@pytest.fixture
def executor_with_remote_queue(remote_queue):
    _, reader = remote_queue
    context = multiprocessing.get_context("spawn")
    death_reader, death_writer = context.Pipe(duplex=False)
    error_reader, error_writer = context.Pipe(duplex=False)
    ready = context.Event()
    proc = context.Process(
        target=_wait_for_parent, args=(death_reader, error_writer, ready)
    )
    proc.start()
    death_reader.close()
    error_writer.close()
    assert ready.wait(30)
    failed = threading.Event()
    executor: Any = MultiprocExecutor.__new__(MultiprocExecutor)
    executor.is_failed = False
    executor.failure_callback = failed.set
    executor.response_mqs = [reader]
    executor.workers = [
        SimpleNamespace(
            proc=proc,
            error_reader=error_reader,
            death_writer=death_writer,
            worker_response_mq=None,
        )
    ]
    errors = []

    def run_monitor():
        try:
            executor.start_worker_monitor(inline=True)
        except Exception as error:
            errors.append(error)

    monitor = threading.Thread(target=run_monitor, daemon=True)
    monitor.start()
    try:
        yield executor, proc, failed, monitor
    finally:
        executor.shutdown()
        proc.join(10)
        if proc.is_alive():
            proc.kill()
            proc.join(10)
        monitor.join(10)
        assert not monitor.is_alive()
        assert not errors
        error_reader.close()
        death_writer.close()
        proc.close()


def test_remote_worker_disconnect_fails_executor(
    remote_queue, executor_with_remote_queue
):
    writer, _ = remote_queue
    executor, proc, failed, monitor = executor_with_remote_queue
    writer.remote_socket.close(linger=0)

    assert failed.wait(5), "remote worker loss did not reach the engine"
    monitor.join(5)
    proc.join(5)
    assert executor.is_failed
    assert not monitor.is_alive()
    assert proc.exitcode == 0, "local worker did not receive the shutdown request"


def test_normal_shutdown_with_remote_worker_is_not_a_failure(
    executor_with_remote_queue,
):
    executor, proc, failed, monitor = executor_with_remote_queue
    executor.shutdown()
    monitor.join(5)
    proc.join(5)

    assert not monitor.is_alive()
    assert proc.exitcode == 0
    assert not executor.is_failed
    assert not failed.is_set()


def test_coordinator_disconnect_notifies_headless_parent(
    error_pipe, remote_queue, monkeypatch
):
    error_reader, error_writer = error_pipe
    writer, reader = remote_queue
    death_reader, death_writer = multiprocessing.Pipe(duplex=False)
    shutdown_requested = threading.Event()
    monitor_threads = []

    def make_monitor_thread(**kwargs):
        thread = threading.Thread(**kwargs)
        monitor_threads.append(thread)
        return thread

    monkeypatch.setattr(
        "vllm.v1.executor.multiproc_executor.Thread", make_monitor_thread
    )
    worker: Any = WorkerProc.__new__(WorkerProc)
    worker.rpc_broadcast_mq = reader
    worker.worker_response_mq = None
    worker.error_writer = error_writer
    worker.monitor_death_pipe(death_reader, shutdown_requested)
    try:
        writer.remote_socket.close(linger=0)
        assert error_reader.poll(5), "coordinator loss did not reach the local executor"
        assert shutdown_requested.wait(5)
    finally:
        death_writer.close()
        assert shutdown_requested.wait(5)
        # The queue context must outlive its monitor's final shutdown call.
        for thread in monitor_threads:
            thread.join(5)
            assert not thread.is_alive()
        death_reader.close()


@pytest.mark.parametrize("timeout", [None, 30])
def test_shutdown_cancels_pending_remote_response(remote_queue, timeout):
    writer, reader = remote_queue
    future = FutureWrapper(deque(), lambda: reader.dequeue(timeout=timeout))
    started = threading.Event()
    errors = []

    def receive():
        started.set()
        try:
            future.result()
        except Exception as error:
            errors.append(error)

    receiver = threading.Thread(target=receive, daemon=True)
    receiver.start()
    assert started.wait(5)
    try:
        reader.shutdown()
        receiver.join(5)
        assert not receiver.is_alive(), "shutdown left the remote response blocked"
        assert len(errors) == 1
        assert isinstance(errors[0], RuntimeError)
    finally:
        if receiver.is_alive():
            # Release a broken implementation without destroying a socket that
            # another thread is still polling.
            writer.enqueue(None)
            receiver.join(5)
        assert not receiver.is_alive()


def test_shutdown_rejects_queued_remote_response(remote_queue):
    writer, reader = remote_queue
    writer.enqueue("stale response")
    reader.shutdown()
    with pytest.raises(RuntimeError):
        reader.dequeue(timeout=5)


def test_remote_receive_timeout_preserves_queue(remote_queue):
    writer, reader = remote_queue
    with pytest.raises(TimeoutError):
        reader.dequeue(timeout=0)
    writer.enqueue("response after timeout")
    assert reader.dequeue(timeout=5) == "response after timeout"
