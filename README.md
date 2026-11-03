# A confirmed consumer-timeout cancellation closes the channel

> [!NOTE]
> Written by Claude Code under the direction of @lukebakken.

On RabbitMQ 4.3 and later, a consumer whose delivery exceeds the consumer timeout is cancelled with `basic.cancel` instead of having its channel closed (`consumer_timeout_response = notify_consumer`, the default). If the client answers that `basic.cancel` with `basic.cancel-ok`, as the broker's `accept_consumer_cancel_ok` capability invites, and the consumer holds a second unacknowledged delivery that expires later, **the broker closes the channel with 406 `PRECONDITION_FAILED`**. A client that does not answer keeps its channel.

The Java, .NET and Go clients send `basic.cancel-ok` when the broker advertises the capability, and pika does from 1.5.0. So the clients that follow the new protocol are the ones whose channels close.

## Reproduce

Requires Docker and Python 3.8 or later.

```bash
docker run -d --name rabbitmq-repro -p 127.0.0.1:5672:5672 -p 127.0.0.1:15672:15672 rabbitmq:4.4-management
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python repro.py --no-cancel-ok
.venv/bin/python repro.py --cancel-ok
```

Wait for the broker to start before the first run (`docker logs rabbitmq-repro` shows "Server startup complete").

`--no-cancel-ok`:

```
run 1: channel still open after 30.0 s
      0.0s  delivery 1 received; never acknowledged
      1.0s  second message published
      1.0s  delivery 2 received; never acknowledged
      2.0s  basic.cancel received from broker
```

`--cancel-ok`:

```
run 1: CHANNEL CLOSED 406
      0.0s  delivery 1 received; never acknowledged
      1.0s  second message published
      1.0s  delivery 2 received; never acknowledged
      2.0s  basic.cancel received from broker
      2.0s  basic.cancel-ok sent
      3.0s  channel closed by broker: 406 PRECONDITION_FAILED - delivery acknowledgement on channel 1 timed out. This timeout value can be configured, see consumers doc guide to learn more
```

Against RabbitMQ 4.4.0 with a quorum queue, `--runs 10` gives 10 of 10 channels closed with `--cancel-ok` and 0 of 10 with `--no-cancel-ok`.

The script sets a 2 s `consumer-timeout` policy on its own queue, consumes with prefetch 10, and holds two deliveries 1 s apart. The second must arrive before the first expires (`--gap` below `--timeout-ms`), or the consumer is already cancelled and it is never delivered. It uses pika 1.4.4, which never sends `basic.cancel-ok` itself, and sends the frame explicitly for `--cancel-ok`, so that frame is the only difference between the two runs. `repro.py --help` lists the options.

With `--queue-type classic` neither mode cancelled the consumer within 90 s, so this reproduces on quorum queues only.

## Why

From `deps/rabbit/src/rabbit_channel.erl` on `v4.4.x` at 9bedff5500 (the same file on `main` at 2e3666cd65):

1. When a delivery's acknowledgement times out, [`handle_consumer_delivery_timeout`](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L2929) sends `basic.cancel` only if the consumer is still in the channel's consumer map; otherwise it falls back to [`handle_consumer_timed_out`](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L2962), which closes the channel with 406.
2. Sending `basic.cancel` ([`notify_consumer_of_cancelation_after_delivery_timeout`](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L2942)) marks the consumer cancel-pending and starts a timer of [`CONSUMER_CANCEL_OK_TIMEOUT`](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L192), 10 s. A later timeout for a consumer that is cancel-pending is ignored.
3. [`basic.cancel-ok`](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L1559) for a cancel-pending consumer cancels it immediately, which removes it from the consumer map. Without one, the [timer](https://github.com/rabbitmq/rabbitmq-server/blob/9bedff5500/deps/rabbit/src/rabbit_channel.erl#L738) does the same after 10 s.

So the second delivery's timeout, at 3 s, finds the consumer still cancel-pending when the client stays silent, and is ignored. When the client has sent `basic.cancel-ok`, the consumer is already gone, and the fallback closes the channel. `notify_consumer` exists to avoid exactly this channel closure, and here it is lost by the client that cooperates with it.

## The question

Should an unacknowledged delivery that belonged to a consumer already cancelled for a consumer timeout close the channel, or be handled as part of that cancellation?
