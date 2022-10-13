# CFX Recorder

This is a python program that can consume messages from an AMQP broker
and archive them into a local file.  

## Basic Installation

Needs Python 3.7 or above.

Install required dependencies:
```
pip install -r requirements_dev.txt -r requirements.txt
```

Install `cfx-recorder` package (from repository root):
```
pip install -e .
```

Verify that you can access the `cfx-recorder` program
```
$ cfx-recorder
ERROR: You must either connect to a broker or load messages from a file
```

## Basic Usage

There are many arguments that you can pass to modify the behavior but the
minimal working command line is:

```
cfx-recorder 
	-b 'amqp://USER:PASSWORD@BROKER:5672' 
	-c 
	-vvvvv 
	--binding 'DESIRED_BINDING_KEY' 
	--queue 'NAME OF TRANSIENT QUEUE' 
	archive

22-10-12 19:54:28.737 DEB cfx_recorder.amqp.background_connection Beginning connection to server XXXX:5672
22-10-12 19:54:28.817 DEB amqp Start from server, version: 0.9, properties: {'capabilities': {'publisher_confirms': True, 'exchange_exchange_bindings': True, 'basic.nack': True, 'consumer_cancel_notify': True, 'connection.blocked': True, 'consumer_priorities': True, 'authentication_failure_close': True, 'per_consumer_qos': True, 'direct_reply_to': True}, 'cluster_name': 'rabbit@localhost', 'copyright': 'Copyright (c) 2007-2022 VMware, Inc. or its affiliates.', 'information': 'Licensed under the MPL 2.0. Website: https://rabbitmq.com', 'platform': 'Erlang/OTP 25.0.3', 'product': 'RabbitMQ', 'version': '3.10.7'}, mechanisms: [b'AMQPLAIN', b'PLAIN'], locales: ['en_US']
22-10-12 19:54:28.892 INF cfx_recorder.amqp.background_connection Successfully connected to host XXXX:5672 with prefetch=100
22-10-12 19:54:28.893 DEB amqp using channel_id: 1
22-10-12 19:54:28.893 INF cfx_recorder.base_link broker-link configured with worker-id: broker-link-751912e6-8736-4c60-90c4-e04cff4d3882
22-10-12 19:54:28.934 DEB amqp Channel open
22-10-12 19:54:29.194 INF cfx_recorder.amqp.background_connection Bound transient queue QQQQQQ to exchange amq.topic with key RRRRRRRRRR
22-10-12 19:54:29.342 INF cfx_recorder.amqp.background_connection Started consumer (tag=amq.ctag-PHw_-CpHZLOTSWFHJDC3oQ) on queue QQQQQQ
22-10-12 19:54:29.343 INF cfx_recorder.base_link Started consuming from mqtt-subscription-timtest (consumer=amq.ctag-PHw_-CpHZLOTSWFHJDC3oQ)
22-10-12 19:54:29.345 INF cfx-recorder Starting main broker-link loop
22-10-12 19:54:29.360 INF cfx_recorder.archive_link Started ArchiveLink. Initial archive file is ./archive/noregion.1665629669.pickle.gz.inprogress
22-10-12 19:54:39.363 DEB cfx_recorder.archive_link Processing 15 messages
22-10-12 19:54:39.370 DEB cfx_recorder.archive_link Flushed 15 messages in 0.00s

^^^^Ctrl-C

22-10-12 19:55:04.527 INF cfx_recorder.archive_link Stopped ArchiveLink.
22-10-12 19:55:04.527 INF cfx-recorder Shutting down amqp connection.
22-10-12 19:55:04.637 DEB cfx_recorder.amqp.background_connection Stop received, shutting down connection
Exiting due to ctrl-c
```

You will see pickle.gz files created in an `archive` folder containing a pickled list of serialized messages captured from the broker.
