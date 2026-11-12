import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pika

from config import RABBITMQ_HOST, RABBITMQ_PORT

MAX_RETRIES = 3


@contextmanager
def queue_channel(*queues):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        blocked_connection_timeout=60,
    ))
    try:
        channel = connection.channel()
        for queue in queues:
            channel.queue_declare(queue=queue, durable=True)
        channel.confirm_delivery()
        yield connection, channel
    finally:
        if connection.is_open:
            connection.close()


def publish(channel, queue, body, headers=None):
    if isinstance(body, dict):
        body = json.dumps(body, ensure_ascii=False).encode("utf-8")
    channel.basic_publish(
        exchange="",
        routing_key=queue,
        body=body,
        mandatory=True,
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json", headers=headers),
    )


def _resend(channel, queue, body, properties, exc, invalid=False):
    headers = dict(properties.headers or {})
    retries = headers.get("retry_count", 0)
    if not isinstance(retries, int) or retries < 0:
        retries = MAX_RETRIES
    destination = queue + ".failed"
    if retries < MAX_RETRIES and not invalid:
        destination = queue
        headers["retry_count"] = retries + 1
    headers["failure_type"] = type(exc).__name__
    logging.error("queue=%s attempt=%d destination=%s error=%s", queue, retries + 1, destination, exc)
    publish(channel, destination, body, headers)


def _task_outputs(process, task, output_queue):
    if output_queue:
        yield from process(task)
    else:
        process(task)


def _wake_connection(connection):
    try:
        connection.add_callback_threadsafe(lambda: None)
    except pika.exceptions.ConnectionWrongStateError:
        pass


def _handle_delivery(connection, channel, executor, queue, output_queue, process, method, properties, body):
    started = time.perf_counter()
    try:
        task = json.loads(body)
        if not isinstance(task, dict):
            raise ValueError("Expected a JSON object")
    except (ValueError, UnicodeError) as exc:
        _resend(channel, queue, body, properties, exc, invalid=True)
    else:
        outputs = _task_outputs(process, task, output_queue)
        try:
            while True:
                future = executor.submit(next, outputs, None)
                # Keep AMQP I/O on this thread while downloads and GDAL/ACOLITE run elsewhere.
                future.add_done_callback(lambda _: _wake_connection(connection))
                while not future.done():
                    connection.process_data_events(time_limit=1)
                    if channel.is_closed:
                        raise pika.exceptions.ChannelWrongStateError("Consumer channel closed during processing")
                try:
                    output = future.result()
                except Exception as exc:
                    _resend(channel, queue, body, properties, exc)
                    break
                if output is None:
                    logging.info("queue=%s scene=%s completed in %.2fs", queue, task.get("scene_name"),
                                 time.perf_counter() - started)
                    break
                publish(channel, output_queue, output)
        finally:
            executor.submit(outputs.close).result()
    channel.basic_ack(delivery_tag=method.delivery_tag)


def run_worker(queue, process, output_queue=None):
    queues = [queue, queue + ".failed"]
    if output_queue:
        queues.append(output_queue)
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            while True:
                try:
                    with queue_channel(*queues) as (connection, channel):
                        channel.basic_qos(prefetch_count=1)
                        logging.info("Waiting for tasks on %s", queue)
                        for method, properties, body in channel.consume(queue):
                            _handle_delivery(connection, channel, executor, queue, output_queue,
                                             process, method, properties, body)
                        logging.warning("Broker cancelled consumer for %s", queue)
                except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker,
                        pika.exceptions.ChannelWrongStateError,
                        pika.exceptions.NackError, pika.exceptions.UnroutableError) as exc:
                    logging.warning("RabbitMQ connection or publish failed for %s: %s", queue, exc)
                time.sleep(5)
    except KeyboardInterrupt:
        logging.info("Stopped worker for %s", queue)
