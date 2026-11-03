# A cancelled consumer's later delivery timeout closes the channel

> [!NOTE]
> Written by Claude Code under the direction of @lukebakken.

On RabbitMQ 4.3 and later, a consumer whose delivery exceeds the consumer timeout is cancelled with `basic.cancel` instead of having its channel closed (`consumer_timeout_response = notify_consumer`, the default). The cancellation returns only the deliveries that have already timed out. **Any delivery the consumer still holds stays on the channel, and when it times out the broker closes the channel with 406 `PRECONDITION_FAILED`**, which is what `notify_consumer` exists to avoid.

Whether a client sees this depends on when its other deliveries expire. Until the broker removes the consumer, a timeout for it is ignored. A client that answers `basic.cancel` with `basic.cancel-ok`, as the `accept_consumer_cancel_ok` capability invites, has the consumer removed at once, so any later expiry closes the channel. A client that does not answer keeps the consumer cancel-pending for 10 s, which hides expiries inside that window but not later ones. The Java, .NET and Go clients send `basic.cancel-ok`, and pika does from 1.5.0.

## Reproduce

Requires Docker and Python 3.8 or later.

```bash
docker run -d --name rabbitmq-repro -p 127.0.0.1:5672:5672 -p 127.0.0.1:15672:15672 rabbitmq:4.4-management
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python repro.py --cancel-ok
.venv/bin/python repro.py --no-cancel-ok
.venv/bin/python repro.py --no-cancel-ok --timeout-ms 20000 --gap 15 --watch 60
```

Wait for the broker to start before the first run (`docker logs rabbitmq-repro` shows "Server startup complete").

The script sets a `consumer-timeout` policy on its own quorum queue, consumes with prefetch 10, and holds two deliveries `--gap` seconds apart, so they time out at different moments. The second must arrive before the first times out (`--gap` below `--timeout-ms`), or the consumer is already cancelled and it is never delivered. It uses pika 1.4.4, which never sends `basic.cancel-ok` itself, and sends the frame explicitly for `--cancel-ok`. `repro.py --help` lists the options.

| Command | Second delivery times out | Result against RabbitMQ 4.4.0 |
| -- | -- | -- |
| `--cancel-ok` | 1 s after the consumer is removed | channel closed with 406 at 3 s, 10 of 10 runs |
| `--no-cancel-ok` | inside the 10 s cancel-pending window | channel stays open, 10 of 10 runs |
| `--no-cancel-ok --timeout-ms 20000 --gap 15` | 5 s after the window ends | channel closed with 406 at 35 s, 2 of 2 runs |

`--cancel-ok` output:

```
run 1: CHANNEL CLOSED 406
      0.0s  delivery 1 received; never acknowledged
      1.0s  second message published
      1.0s  delivery 2 received; never acknowledged
      2.0s  basic.cancel received from broker
      2.0s  basic.cancel-ok sent
      3.0s  channel closed by broker: 406 PRECONDITION_FAILED - delivery acknowledgement on channel 1 timed out. This timeout value can be configured, see consumers doc guide to learn more
```

In the `--cancel-ok` case, a `basic.get` from another connection at 2.5 s returns only the first message: the cancellation returned the delivery that had timed out, and the consumer's channel still held the second when it timed out at 3 s. In the second case, both messages were back in the queue, with no consumer, at the first check after the window, 35 s in.

With `--queue-type classic`, neither mode cancelled the consumer within 90 s, so this reproduces on quorum queues only.

## Late acknowledgements

`late_ack.py` holds one delivery past its timeout and then acknowledges it, to show that a late acknowledgement is not what closes the channel:

```bash
.venv/bin/python late_ack.py --cancel-ok --ack-at 4
.venv/bin/python late_ack.py --ack-at 4
.venv/bin/python late_ack.py --ack-at 15
.venv/bin/python late_ack.py --cancel-ok --ack-at 4 --nack
.venv/bin/python late_ack.py --cancel-ok --ack-at 4 --bogus-tag
```

| Command | Channel | Queue afterwards |
| -- | -- | -- |
| `--cancel-ok --ack-at 4` | open | the message is back |
| `--ack-at 4`, inside the 10 s window | open | the message is back |
| `--ack-at 15`, after the window | open | the message is back |
| `--cancel-ok --ack-at 4 --nack` | open | the message is back |
| `--bogus-tag`, tag 999 never issued | closed, 406 `unknown delivery tag 999` | the message is back |

The channel ignores a late acknowledgement for a delivery it reclaimed for a timed-out consumer, but still closes for a tag it never issued. So it knows the consumer was cancelled for a timeout when an acknowledgement arrives, and not when one of the consumer's remaining deliveries times out.

## Why

From `deps/rabbit/src/rabbit_channel.erl` on `v4.4.x` at 9bedff5500 (the same file on `main` at 2e3666cd65):

1. When a delivery's acknowledgement times out, [`handle_consumer_delivery_timeout`](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L2929) sends `basic.cancel` only if the consumer is still in the channel's consumer map; otherwise it falls back to [`handle_consumer_timed_out`](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L2962), which closes the channel with 406.
2. Sending `basic.cancel` ([`notify_consumer_of_cancelation_after_delivery_timeout`](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L2942)) marks the consumer cancel-pending and starts a timer of [`CONSUMER_CANCEL_OK_TIMEOUT`](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L192), 10 s. A later timeout for a consumer that is cancel-pending is ignored.
3. [`basic.cancel-ok`](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L1559) for a cancel-pending consumer cancels it immediately, which removes it from the consumer map. Without one, the [timer](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L738) does the same after 10 s.

Once the consumer is out of the map, the next timeout of a delivery it still holds takes the fallback in step 1.

## The question

Should a delivery from a consumer already cancelled for a timeout be covered by that cancellation instead of closing the channel?
