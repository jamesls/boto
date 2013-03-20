# Copyright (c) 2013 Amazon.com, Inc. or its affiliates.  All Rights Reserved
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.
#
import time
from threading import Thread
from Queue import Queue
from Queue import Empty as _QueueEmpty


class Empty(Exception):
    pass


class Config(object):
    def __init__(self, max_read_messages=10, max_poll_seconds=20,
                 batch_size_max=10, batch_send_delay=0.5,
                 batch_send_poll_time=5,
                 send_delay_time=0):
        self.max_read_messages = max_read_messages
        self.max_poll_seconds = max_poll_seconds
        self.batch_size_max = batch_size_max
        self.batch_send_delay = batch_send_delay
        self.batch_send_poll_time = batch_send_poll_time
        self.send_delay_time = send_delay_time


class MultiQueue(object):
    def __init__(self, queues, config=Config()):
        self._queues = queues
        self._com_queues = {}
        self._threads = []
        self._name_to_queue = {}
        self._recv_queue = Queue()
        self._send_queues = {}
        self._config = config
        for queue in queues:
            qp = QueuePoller(self._recv_queue, queue, self._config)
            qp.daemon = True
            qp.start()
            self._threads.append(qp)

            send_queue = Queue()
            self._send_queues[queue.name] = send_queue
            self._name_to_queue[queue.name] = queue
            batched_sender = BatchedSender(send_queue, queue)
            batched_sender.daemon = True
            batched_sender.start()
            self._threads.append(batched_sender)

    def get(self, block=True):
        # Verify block=False raises Empty()
        return self._recv_queue.get(block=block)

    def put(self, body, queue_name):
        q = self._name_to_queue[queue_name]
        message = q.new_message(body)
        self._send_queues[queue_name].put(message)

    def shutdown(self):
        for thread in self._threads:
            thread.should_continue = False
        for thread in self._threads:
            thread.join()


class QueuePoller(Thread):
    def __init__(self, com_queue, sqs_queue, config):
        super(QueuePoller, self).__init__()
        self._com_queue = com_queue
        self._sqs_queue = sqs_queue
        self._config = config
        self.should_continue = True

    def run(self):
        max_read_messages = self._config.max_read_messages
        max_poll_seconds = self._config.max_poll_seconds
        while self.should_continue:
            messages = self._sqs_queue.get_messages(
                num_messages=max_read_messages,
                wait_time_seconds=max_poll_seconds)
            if messages:
                for message in messages:
                    self._com_queue.put(message)


class BatchedSender(Thread):
    def __init__(self, send_queue, sqs_queue, wait_time=0.5):
        super(BatchedSender, self).__init__()
        self._send_queue = send_queue
        self._sqs_queue = sqs_queue
        self._wait_time = wait_time
        self._countdown = None
        self._accumulated = []
        self.should_continue = True

    def run(self):
        while self.should_continue:
            if self._countdown is None:
                # Wait until we get a message.
                try:
                    message = self._send_queue.get(timeout=5)
                except _QueueEmpty:
                    continue
                else:
                    self._countdown = self._wait_time
                    self._accumulated.append(message)
            else:
                start = time.time()
                try:
                    message = self._send_queue.get(timeout=self._countdown)
                    self._accumulated.append(message)
                except _QueueEmpty:
                    self._send_messages()
                    continue
                duration = time.time() - start
                self._countdown -= duration
                if self._countdown <= 0 or len(self._accumulated) >= BATCH_SIZE_MAX:
                    self._send_messages()

    def _send_messages(self):
        # _send_messages is responsible for sending all the
        # accumulated messages as well as resetting the state
        # variables.
        messages = []
        for i, message in enumerate(self._accumulated):
            messages.append((i, message.get_body_encoded(), 0))
        self._sqs_queue.write_batch(messages)
        print("Sent %s messages" % len(messages))
        self._countdown = None
        self._accumulated = []


def demo():
    import boto
    sqs = boto.connect_sqs()
    print("Creating queues")
    num_queues = 5
    queues = [sqs.create_queue('q%s' % i) for i in range(num_queues)]
    num_messages = 100
    print("Creating multiqueue")
    mq = MultiQueue(queues)
    print("Adding messages to queues.")
    for queue in queues:
        for i in range(num_messages):
            mq.put('foo', queue.name)
    print("Retrieving messages.")
    count = 0
    for i in range(num_queues * num_messages):
        message = mq.get()
        # TODO: why is get_body() not returning the original body?
        # I think this is a moto bug?  Also I noticed moto is not
        # thread safe so we won't necessarily get num_queues * num_messages
        # responses back.
        count += 1
        print("Got message", message.get_body())
        print("From queue:", message.queue)
        print("Total received:", count)
    print("Shutting down...")
    mq.shutdown()


if __name__ == '__main__':
    demo()
