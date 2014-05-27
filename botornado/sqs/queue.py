# Copyright (c) 2006-2009 Mitch Garnaat http://garnaat.org/
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

"""
Represents an SQS Queue
"""

import urlparse
from boto.sqs.message import Message

from boto.sqs.queue import *

import tornado.gen

class AsyncQueue(Queue):

    def __init__(self, connection=None, url=None, message_class=Message):
        self.connection = connection
        self.url = url
        self.message_class = message_class
        self.visibility_timeout = None

    @tornado.gen.coroutine
    def get_attributes(self, attributes='All'):
        """
        Retrieves attributes about this queue object and returns
        them in an Attribute instance (subclass of a Dictionary).

        :type attributes: string
        :param attributes: String containing one of:
                           ApproximateNumberOfMessages,
                           ApproximateNumberOfMessagesNotVisible,
                           VisibilityTimeout,
                           CreatedTimestamp,
                           LastModifiedTimestamp,
                           Policy
                           ReceiveMessageWaitTimeSeconds
        :rtype: Attribute object
        :return: An Attribute object which is a mapping type holding the
                 requested name/value pairs
        """
        result = yield self.connection.get_queue_attributes(self, attributes)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def set_attribute(self, attribute, value):
        """
        Set a new value for an attribute of the Queue.

        :type attribute: String
        :param attribute: The name of the attribute you want to set.  The
                           only valid value at this time is: VisibilityTimeout
        :type value: int
        :param value: The new value for the attribute.
            For VisibilityTimeout the value must be an
            integer number of seconds from 0 to 86400.

        :rtype: bool
        :return: True if successful, otherwise False.
        """
        result = yield self.connection.set_queue_attribute(self, attribute, value)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def get_timeout(self):
        """
        Get the visibility timeout for the queue.

        :rtype: int
        :return: The number of seconds as an integer.
        """
        a = yield self.get_attributes('VisibilityTimeout')
        raise tornado.gen.Return(int(a['VisibilityTimeout']))

    @tornado.gen.coroutine
    def set_timeout(self, visibility_timeout):
        """
        Set the visibility timeout for the queue.

        :type visibility_timeout: int
        :param visibility_timeout: The desired timeout in seconds
        """
        retval = yield self.set_attribute('VisibilityTimeout', visibility_timeout)
        if retval:
            self.visibility_timeout = visibility_timeout
        raise tornado.gen.Return(retval)

    @tornado.gen.coroutine
    def add_permission(self, label, aws_account_id, action_name):
        """
        Add a permission to a queue.

        :type label: str or unicode
        :param label: A unique identification of the permission you are setting.
            Maximum of 80 characters ``[0-9a-zA-Z_-]``
            Example, AliceSendMessage

        :type aws_account_id: str or unicode
        :param principal_id: The AWS account number of the principal who
            will be given permission.  The principal must have an AWS account,
            but does not need to be signed up for Amazon SQS. For information
            about locating the AWS account identification.

        :type action_name: str or unicode
        :param action_name: The action.  Valid choices are:
            SendMessage|ReceiveMessage|DeleteMessage|
            ChangeMessageVisibility|GetQueueAttributes|*

        :rtype: bool
        :return: True if successful, False otherwise.

        """
        result = yield self.connection.add_permission(self, label, aws_account_id,
                                              action_name)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def remove_permission(self, label):
        """
        Remove a permission from a queue.

        :type label: str or unicode
        :param label: The unique label associated with the permission
            being removed.

        :rtype: bool
        :return: True if successful, False otherwise.
        """
        result = yield self.connection.remove_permission(self, label)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def read(self, visibility_timeout=None, wait_time_seconds=None,
             message_attributes=None):
        """
        Read a single message from the queue.

        :type visibility_timeout: int
        :param visibility_timeout: The timeout for this message in seconds

        :type wait_time_seconds: int
        :param wait_time_seconds: The duration (in seconds) for which the call
            will wait for a message to arrive in the queue before returning.
            If a message is available, the call will return sooner than
            wait_time_seconds.

        :type message_attributes: list
        :param message_attributes: The name(s) of additional message
            attributes to return. The default is to return no additional
            message attributes. Use ``['All']`` or ``['.*']`` to return all.

        :rtype: :class:`boto.sqs.message.Message`
        :return: A single message or None if queue is empty
        """
        rs = yield self.get_messages(1, visibility_timeout,
                               wait_time_seconds=wait_time_seconds,
                               message_attributes=message_attributes)
        if len(rs) == 1:
            raise tornado.gen.Return(rs[0])
        else:
            raise tornado.gen.Return(None)

    @tornado.gen.coroutine
    def write(self, message, delay_seconds=None):
        """
        Add a single message to the queue.

        :type message: Message
        :param message: The message to be written to the queue

        :rtype: :class:`boto.sqs.message.Message`
        :return: The :class:`boto.sqs.message.Message` object that was written.
        """
        new_msg = yield self.connection.send_message(self,
            message.get_body_encoded(), delay_seconds=delay_seconds,
            message_attributes=message.message_attributes)
        message.id = new_msg.id
        message.md5 = new_msg.md5
        raise tornado.gen.Return(message)

    @tornado.gen.coroutine
    def write_batch(self, messages):
        """
        Delivers up to 10 messages in a single request.

        :type messages: List of lists.
        :param messages: A list of lists or tuples.  Each inner
            tuple represents a single message to be written
            and consists of and ID (string) that must be unique
            within the list of messages, the message body itself
            which can be a maximum of 64K in length, an
            integer which represents the delay time (in seconds)
            for the message (0-900) before the message will
            be delivered to the queue, and an optional dict of
            message attributes like those passed to ``send_message``
            in the connection class.
        """
        result = yield self.connection.send_message_batch(self, messages)
        raise tornado.gen.Return(result)

    def new_message(self, body='', **kwargs):
        """
        Create new message of appropriate class.

        :type body: message body
        :param body: The body of the newly created message (optional).

        :rtype: :class:`boto.sqs.message.Message`
        :return: A new Message object
        """
        m = self.message_class(self, body, **kwargs)
        m.queue = self
        return m

    # get a variable number of messages, returns a list of messages
    @tornado.gen.coroutine
    def get_messages(self, num_messages=1, visibility_timeout=None,
                     attributes=None, wait_time_seconds=None,
                     message_attributes=None):
        """
        Get a variable number of messages.

        :type num_messages: int
        :param num_messages: The maximum number of messages to read from
            the queue.

        :type visibility_timeout: int
        :param visibility_timeout: The VisibilityTimeout for the messages read.

        :type attributes: str
        :param attributes: The name of additional attribute to return
            with response or All if you want all attributes.  The
            default is to return no additional attributes.  Valid
            values: All SenderId SentTimestamp ApproximateReceiveCount
            ApproximateFirstReceiveTimestamp

        :type wait_time_seconds: int
        :param wait_time_seconds: The duration (in seconds) for which the call
            will wait for a message to arrive in the queue before returning.
            If a message is available, the call will return sooner than
            wait_time_seconds.

        :type message_attributes: list
        :param message_attributes: The name(s) of additional message
            attributes to return. The default is to return no additional
            message attributes. Use ``['All']`` or ``['.*']`` to return all.

        :rtype: list
        :return: A list of :class:`boto.sqs.message.Message` objects.
        """
        result = yield self.connection.receive_message(
            self, number_messages=num_messages,
            visibility_timeout=visibility_timeout, attributes=attributes,
            wait_time_seconds=wait_time_seconds,
            message_attributes=message_attributes)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def delete_message(self, message):
        """
        Delete a message from the queue.

        :type message: :class:`boto.sqs.message.Message`
        :param message: The :class:`boto.sqs.message.Message` object to delete.

        :rtype: bool
        :return: True if successful, False otherwise
        """
        result = yield self.connection.delete_message(self, message)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def delete_message_batch(self, messages):
        """
        Deletes a list of messages in a single request.

        :type messages: List of :class:`boto.sqs.message.Message` objects.
        :param messages: A list of message objects.
        """
        result = yield self.connection.delete_message_batch(self, messages)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def change_message_visibility_batch(self, messages):
        """
        A batch version of change_message_visibility that can act
        on up to 10 messages at a time.

        :type messages: List of tuples.
        :param messages: A list of tuples where each tuple consists
            of a :class:`boto.sqs.message.Message` object and an integer
            that represents the new visibility timeout for that message.
        """
        result = yield self.connection.change_message_visibility_batch(self, messages)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def delete(self):
        """
        Delete the queue.
        """
        result = yield self.connection.delete_queue(self)
        raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def clear(self, page_size=10, vtimeout=10):
        """Utility function to remove all messages from a queue"""
        n = 0
        l = yield self.get_messages(page_size, vtimeout)
        while l:
            for m in l:
                yield self.delete_message(m)
                n += 1
            l = yield self.get_messages(page_size, vtimeout)
        raise tornado.gen.Return(n)

    @tornado.gen.coroutine
    def count(self, page_size=10, vtimeout=10):
        """
        Utility function to count the number of messages in a queue.
        Note: This function now calls GetQueueAttributes to obtain
        an 'approximate' count of the number of messages in a queue.
        """
        a = yield self.get_attributes('ApproximateNumberOfMessages')
        raise tornado.gen.Return(int(a['ApproximateNumberOfMessages']))

    @tornado.gen.coroutine
    def count_slow(self, page_size=10, vtimeout=10):
        """
        Deprecated.  This is the old 'count' method that actually counts
        the messages by reading them all.  This gives an accurate count but
        is very slow for queues with non-trivial number of messasges.
        Instead, use get_attribute('ApproximateNumberOfMessages') to take
        advantage of the new SQS capability.  This is retained only for
        the unit tests.
        """
        n = 0
        l = yield self.get_messages(page_size, vtimeout)
        while l:
            for m in l:
                n += 1
            l = yield self.get_messages(page_size, vtimeout)
        raise tornado.gen.Return(n)

    @tornado.gen.coroutine
    def dump(self, file_name, page_size=10, vtimeout=10, sep='\n'):
        """Utility function to dump the messages in a queue to a file
        NOTE: Page size must be < 10 else SQS errors"""
        fp = open(file_name, 'wb')
        n = 0
        l = yield self.get_messages(page_size, vtimeout)
        while l:
            for m in l:
                fp.write(m.get_body())
                if sep:
                    fp.write(sep)
                n += 1
            l = yield self.get_messages(page_size, vtimeout)
        fp.close()
        raise tornado.gen.Return(n)

    @tornado.gen.coroutine
    def save_to_file(self, fp, sep='\n'):
        """
        Read all messages from the queue and persist them to file-like object.
        Messages are written to the file and the 'sep' string is written
        in between messages.  Messages are deleted from the queue after
        being written to the file.
        Returns the number of messages saved.
        """
        n = 0
        m = yield self.read()
        while m:
            n += 1
            fp.write(m.get_body())
            if sep:
                fp.write(sep)
            yield self.delete_message(m)
            m = yield self.read()
        raise tornado.gen.Return(n)

    @tornado.gen.coroutine
    def save_to_filename(self, file_name, sep='\n'):
        """
        Read all messages from the queue and persist them to local file.
        Messages are written to the file and the 'sep' string is written
        in between messages.  Messages are deleted from the queue after
        being written to the file.
        Returns the number of messages saved.
        """
        fp = open(file_name, 'wb')
        n = yield self.save_to_file(fp, sep)
        fp.close()
        raise tornado.gen.Return(n)

    # for backwards compatibility
    save = save_to_filename

    @tornado.gen.coroutine
    def save_to_s3(self, bucket):
        """
        Read all messages from the queue and persist them to S3.
        Messages are stored in the S3 bucket using a naming scheme of::

            <queue_id>/<message_id>

        Messages are deleted from the queue after being saved to S3.
        Returns the number of messages saved.
        """
        raise BotoClientError('Not Implemented')

    @tornado.gen.coroutine
    def load_from_s3(self, bucket, prefix=None):
        """
        Load messages previously saved to S3.
        """
        raise BotoClientError('Not Implemented')

    @tornado.gen.coroutine
    def load_from_file(self, fp, sep='\n'):
        """Utility function to load messages from a file-like object to a queue"""
        n = 0
        body = ''
        l = fp.readline()
        while l:
            if l == sep:
                m = Message(self, body)
                yield self.write(m)
                n += 1
                print 'writing message %d' % n
                body = ''
            else:
                body = body + l
            l = fp.readline()
        raise tornado.gen.Return(n)

    @tornado.gen.coroutine
    def load_from_filename(self, file_name, sep='\n'):
        """Utility function to load messages from a local filename to a queue"""
        fp = open(file_name, 'rb')
        n = yield self.load_from_file(fp, sep)
        fp.close()
        raise tornado.gen.Return(n)

    # for backward compatibility
    load = load_from_filename

# vim:set ft=python sw=4 :
