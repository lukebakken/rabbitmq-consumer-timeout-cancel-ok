"""
Acknowledge a delivery after its consumer timeout, and report whether the channel survives.

One delivery on a quorum queue exceeds the consumer timeout and the broker cancels the consumer
with `basic.cancel`. The client then acknowledges (or nacks) the delivery late, at `--ack-at`
seconds. The script reports whether the broker closed the channel and what is left in the queue.

`--bogus-tag` acknowledges delivery tag 999, which the broker never issued, as a control: that
one closes the channel with 406 `unknown delivery tag`.
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


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--cancel-ok', action='store_true',
                        help='answer basic.cancel with basic.cancel-ok')
    parser.add_argument('--ack-at', type=float, required=True,
                        help='seconds after the delivery to send the acknowledgement')
    settle = parser.add_mutually_exclusive_group()
    settle.add_argument('--nack', action='store_true', help='send basic.nack with requeue')
    settle.add_argument('--bogus-tag', action='store_true',
                        help='acknowledge delivery tag 999, never issued')
    parser.add_argument('--timeout-ms', type=int, default=2000,
                        help='consumer-timeout policy value (default 2000)')
    args = parser.parse_args()

    queue = f'late-ack-{uuid.uuid4().hex}'
    api('PUT', f'policies/%2F/{queue}', {
        'pattern': f'^{queue}$', 'apply-to': 'queues',
        'definition': {'consumer-timeout': args.timeout_ms}})
    conn = pika.BlockingConnection(AMQP)
    ch = conn.channel()
    ch.basic_qos(prefetch_count=10)
    ch.queue_declare(queue, durable=True, arguments={'x-queue-type': 'quorum'})
    start = [None]
    closed = []
    tags = []

    def note(what):
        print(f'    {time.monotonic() - start[0]:5.1f}s  {what}', flush=True)

    def on_broker_cancel(frame):
        note('basic.cancel received from broker')
        if args.cancel_ok:
            # Private pika call: 1.4.4 has no public way to send basic.cancel-ok.
            ch._impl._send_method(spec.Basic.CancelOk(consumer_tag=frame.method.consumer_tag))
            note('basic.cancel-ok sent')

    def on_channel_closed(_ch, reason):
        if isinstance(reason, pika.exceptions.ChannelClosedByBroker):
            closed.append(reason)
            note(f'channel closed by broker: {reason.reply_code} {reason.reply_text}')

    def on_delivery(_ch, method, _props, _body):
        start[0] = start[0] or time.monotonic()
        tags.append(method.delivery_tag)
        note(f'delivery {method.delivery_tag} received; not acknowledged yet')

    ch._impl.add_on_cancel_callback(on_broker_cancel)
    ch._impl.add_on_close_callback(on_channel_closed)
    ch.basic_consume(queue, on_delivery)

    publisher = pika.BlockingConnection(AMQP)
    publisher.channel().basic_publish('', queue, b'the message')
    publisher.close()
    try:
        while start[0] is None:
            conn.process_data_events(time_limit=0.05)
        while time.monotonic() - start[0] < args.ack_at and not closed:
            conn.process_data_events(time_limit=0.05)
        if not closed:
            if args.bogus_tag:
                ch._impl.basic_ack(999)
                note('basic.ack for delivery 999 sent')
            elif args.nack:
                ch._impl.basic_nack(tags[0], requeue=True)
                note(f'basic.nack(requeue) for delivery {tags[0]} sent')
            else:
                ch._impl.basic_ack(tags[0])
                note(f'basic.ack for delivery {tags[0]} sent')
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not closed:
                conn.process_data_events(time_limit=0.05)
    finally:
        if conn.is_open:
            conn.close()
    checker = pika.BlockingConnection(AMQP)
    left = []
    while True:
        method, _props, body = checker.channel().basic_get(queue, auto_ack=True)
        if method is None:
            break
        left.append(body.decode())
    checker.channel().queue_delete(queue)
    checker.close()
    api('DELETE', f'policies/%2F/{queue}')
    print('channel:', f'CLOSED {closed[0].reply_code}' if closed
          else 'still open 5 s after the late acknowledgement')
    print('queue afterwards:', left or 'empty')


if __name__ == '__main__':
    main()
