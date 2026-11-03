"""
Reproduce a channel closed with 406 when a cancelled consumer's later delivery times out.

One consumer holds two unacknowledged deliveries whose acknowledgement timeouts expire at
different moments. RabbitMQ (4.3+, `consumer_timeout_response = notify_consumer`, the default)
cancels the consumer with `basic.cancel` when the first one expires, and closes the channel with
406 PRECONDITION_FAILED when the second expires after the consumer has been removed:

- `--cancel-ok` answers `basic.cancel` with `basic.cancel-ok`, so the consumer is removed at once.
- `--no-cancel-ok` does not answer, so the consumer stays cancel-pending for 10 s, and only an
  expiry after that window closes the channel.

pika 1.4.4 never sends `basic.cancel-ok` itself, so with `--cancel-ok` this script sends it.
"""
import argparse
import base64
import json
import time
import urllib.request
import uuid

import pika
from pika import spec

AMQP = pika.ConnectionParameters('127.0.0.1', 5672)
API = 'http://127.0.0.1:15672/api/'
AUTH = 'Basic ' + base64.b64encode(b'guest:guest').decode()


def api(method, path, body=None):
    request = urllib.request.Request(
        API + path, method=method, data=None if body is None else json.dumps(body).encode(),
        headers={'Authorization': AUTH, 'Content-Type': 'application/json'})
    urllib.request.urlopen(request).close()


def run_once(queue_type, cancel_ok, timeout_ms, gap, watch):
    queue = f'repro-{uuid.uuid4().hex}'
    api('PUT', f'policies/%2F/{queue}', {
        'pattern': f'^{queue}$', 'apply-to': 'queues',
        'definition': {'consumer-timeout': timeout_ms}})
    conn = pika.BlockingConnection(AMQP)
    ch = conn.channel()
    ch.basic_qos(prefetch_count=10)
    ch.queue_declare(queue, durable=True, arguments={'x-queue-type': queue_type})
    events = []
    start = [None]

    def note(what):
        events.append((round(time.monotonic() - start[0], 1), what))

    def on_delivery(_ch, method, _props, _body):
        start[0] = start[0] or time.monotonic()
        note(f'delivery {method.delivery_tag} received; never acknowledged')

    def on_broker_cancel(frame):
        note('basic.cancel received from broker')
        if cancel_ok:
            # Private pika call: 1.4.4 has no public way to send basic.cancel-ok.
            ch._impl._send_method(spec.Basic.CancelOk(consumer_tag=frame.method.consumer_tag))
            note('basic.cancel-ok sent')

    closed = []

    def on_channel_closed(_ch, reason):
        if isinstance(reason, pika.exceptions.ChannelClosedByBroker):
            closed.append(reason)
            note(f'channel closed by broker: {reason.reply_code} {reason.reply_text}')

    ch._impl.add_on_cancel_callback(on_broker_cancel)
    ch._impl.add_on_close_callback(on_channel_closed)
    ch.basic_consume(queue, on_delivery)

    publisher = pika.BlockingConnection(AMQP)
    publisher.channel().basic_publish('', queue, b'first')
    while start[0] is None:
        conn.process_data_events(time_limit=0.1)
    while time.monotonic() - start[0] < gap:
        conn.process_data_events(time_limit=0.1)
    publisher.channel().basic_publish('', queue, b'second')
    note('second message published')
    publisher.close()

    try:
        while not closed and time.monotonic() - start[0] < watch:
            conn.process_data_events(time_limit=0.5)
    except pika.exceptions.ChannelClosedByBroker:
        pass  # Recorded by on_channel_closed.
    finally:
        if conn.is_open:
            conn.close()
        cleanup = pika.BlockingConnection(AMQP)
        cleanup.channel().queue_delete(queue)
        cleanup.close()
        api('DELETE', f'policies/%2F/{queue}')
    outcome = (f'CHANNEL CLOSED {closed[0].reply_code}' if closed else
               f'channel still open after {watch} s')
    return outcome, events


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    reply = parser.add_mutually_exclusive_group(required=True)
    reply.add_argument('--cancel-ok', dest='cancel_ok', action='store_true',
                       help='answer basic.cancel with basic.cancel-ok')
    reply.add_argument('--no-cancel-ok', dest='cancel_ok', action='store_false',
                       help='do not answer basic.cancel')
    parser.add_argument('--queue-type', choices=('quorum', 'classic'), default='quorum')
    parser.add_argument('--runs', type=int, default=1)
    parser.add_argument('--timeout-ms', type=int, default=2000,
                        help='consumer-timeout policy value (default 2000)')
    parser.add_argument('--gap', type=float, default=1.0,
                        help='seconds between the first delivery and publishing the second; '
                        'must be less than the timeout, so both are delivered before the first expires')
    parser.add_argument('--watch', type=float, default=30.0,
                        help='seconds after the first delivery to wait for a channel close')
    args = parser.parse_args()
    for n in range(1, args.runs + 1):
        outcome, events = run_once(args.queue_type, args.cancel_ok, args.timeout_ms,
                                   args.gap, args.watch)
        print(f'run {n}: {outcome}')
        for at, what in events:
            print(f'    {at:5.1f}s  {what}')


if __name__ == '__main__':
    main()
