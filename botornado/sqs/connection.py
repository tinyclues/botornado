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

from boto.connection import AWSQueryConnection
from boto.sqs.regioninfo import SQSRegionInfo
from boto.sqs.queue import Queue
from boto.sqs.message import Message
from boto.sqs.attributes import Attributes
from boto.exception import SQSError

from boto.sqs.connection import *
import boto.regioninfo
import botornado.connection
import botornado.sqs.queue
import botornado.sqs.message

class AsyncSQSConnection(botornado.connection.AsyncAWSQueryConnection, SQSConnection):
    """
    A Connection to the SQS Service.
    """

    def __init__(self, region=None, **kwargs):
        if not region:
           region = boto.regioninfo.RegionInfo(self, self.DefaultRegionName,
                                               self.DefaultRegionEndpoint, connection_cls=self.__class__)
        self.region = region
        botornado.connection.AsyncAWSQueryConnection.__init__(self, host=self.region.endpoint, **kwargs)

    def create_queue(self, queue_name, visibility_timeout=None, callback=None):
        """
        Create an SQS Queue.

        :type queue_name: str or unicode
        :param queue_name: The name of the new queue.  Names are scoped to
                           an account and need to be unique within that
                           account.  Calling this method on an existing
                           queue name will not return an error from SQS
                           unless the value for visibility_timeout is
                           different than the value of the existing queue
                           of that name.  This is still an expensive operation,
                           though, and not the preferred way to check for
                           the existence of a queue.  See the
                           :func:`boto.sqs.connection.SQSConnection.lookup` method.

        :type visibility_timeout: int
        :param visibility_timeout: The default visibility timeout for all
                                   messages written in the queue.  This can
                                   be overridden on a per-message.

        :rtype: :class:`boto.sqs.queue.Queue`
        :return: The newly created queue.

        """
        params = {'QueueName': queue_name}
        if visibility_timeout:
            params['DefaultVisibilityTimeout'] = '%d' % (visibility_timeout,)
        return self.get_object('CreateQueue', params, botornado.sqs.queue.AsyncQueue, callback=callback)

    def delete_queue(self, queue, force_deletion=False, callback=None):
        """
        Delete an SQS Queue.

        :type queue: A Queue object
        :param queue: The SQS queue to be deleted
        
        :type force_deletion: Boolean
        :param force_deletion: Normally, SQS will not delete a queue that
                               contains messages.  However, if the
                               force_deletion argument is True, the
                               queue will be deleted regardless of whether
                               there are messages in the queue or not.
                               USE WITH CAUTION.  This will delete all
                               messages in the queue as well.
                               
        :rtype: bool
        :return: True if the command succeeded, False otherwise
        """
        return self.get_status('DeleteQueue', None, queue.id, callback=callback)

    def get_queue_attributes(self, queue, attribute='All', callback=None):
        """
        Gets one or all attributes of a Queue
        
        :type queue: A Queue object
        :param queue: The SQS queue to be deleted

        :type attribute: str
        :type attribute: The specific attribute requested.  If not supplied,
                         the default is to return all attributes.
                         Valid attributes are:
                         
                         ApproximateNumberOfMessages|
                         ApproximateNumberOfMessagesNotVisible|
                         VisibilityTimeout|
                         CreatedTimestamp|
                         LastModifiedTimestamp|
                         Policy
                         
        :rtype: :class:`boto.sqs.attributes.Attributes`
        :return: An Attributes object containing request value(s).
        """
        params = {'AttributeName' : attribute}
        return self.get_object('GetQueueAttributes', params,
                               Attributes, queue.id, callback=callback)

    def set_queue_attribute(self, queue, attribute, value, callback=None):
        params = {'Attribute.Name' : attribute, 'Attribute.Value' : value}
        return self.get_status('SetQueueAttributes', params, queue.id, callback=callback)

    def receive_message(self, queue, number_messages=1,
                        visibility_timeout=None, attributes=None, callback=None):
        """
        Read messages from an SQS Queue.

        :type queue: A Queue object
        :param queue: The Queue from which messages are read.
        
        :type number_messages: int
        :param number_messages: The maximum number of messages to read
                                (default=1)
        
        :type visibility_timeout: int
        :param visibility_timeout: The number of seconds the message should
                                   remain invisible to other queue readers
                                   (default=None which uses the Queues default)

        :type attributes: str
        :param attributes: The name of additional attribute to return
                           with response or All if you want all attributes.
                           The default is to return no additional attributes.
                           Valid values:
                           
                           All|SenderId|SentTimestamp|
                           ApproximateReceiveCount|
                           ApproximateFirstReceiveTimestamp
        
        :rtype: list
        :return: A list of :class:`boto.sqs.message.Message` objects.
        """
        params = {'MaxNumberOfMessages' : number_messages}
        if visibility_timeout:
            params['VisibilityTimeout'] = visibility_timeout
        if attributes:
            self.build_list_params(params, attributes, 'AttributeName')
        return self.get_list('ReceiveMessage', params,
                             [('Message', queue.message_class)],
                             queue.id, queue, callback=callback)

    def delete_message(self, queue, message, callback=None):
        """
        Delete a message from a queue.

        :type queue: A :class:`boto.sqs.queue.Queue` object
        :param queue: The Queue from which messages are read.
        
        :type message: A :class:`boto.sqs.message.Message` object
        :param message: The Message to be deleted
        
        :rtype: bool
        :return: True if successful, False otherwise.
        """
        params = {'ReceiptHandle' : message.receipt_handle}
        return self.get_status('DeleteMessage', params, queue.id, callback=callback)

    def delete_message_from_handle(self, queue, receipt_handle, callback=None):
        """
        Delete a message from a queue, given a receipt handle.

        :type queue: A :class:`boto.sqs.queue.Queue` object
        :param queue: The Queue from which messages are read.
        
        :type receipt_handle: str
        :param receipt_handle: The receipt handle for the message
        
        :rtype: bool
        :return: True if successful, False otherwise.
        """
        params = {'ReceiptHandle' : receipt_handle}
        return self.get_status('DeleteMessage', params, queue.id, callback=callback)

    def send_message(self, queue, message_content, callback=None):
        params = {'MessageBody' : message_content}
        return self.get_object('SendMessage', params, botornado.sqs.message.AsyncMessage,
                               queue.id, verb='POST', callback=callback)

    def change_message_visibility(self, queue, receipt_handle,
                                  visibility_timeout, callback=None):
        """
        Extends the read lock timeout for the specified message from
        the specified queue to the specified value.

        :type queue: A :class:`boto.sqs.queue.Queue` object
        :param queue: The Queue from which messages are read.
        
        :type receipt_handle: str
        :param queue: The receipt handle associated with the message whose
                      visibility timeout will be changed.
        
        :type visibility_timeout: int
        :param visibility_timeout: The new value of the message's visibility
                                   timeout in seconds.
        """
        params = {'ReceiptHandle' : receipt_handle,
                  'VisibilityTimeout' : visibility_timeout}
        return self.get_status('ChangeMessageVisibility', params, queue.id, callback=callback)

    def get_all_queues(self, prefix='', callback=None):
        params = {}
        if prefix:
            params['QueueNamePrefix'] = prefix
        return self.get_list('ListQueues', params, [('QueueUrl', botornado.sqs.queue.AsyncQueue)], callback=callback)
        
    def get_queue(self, queue_name, callback=None):
        def queue_got(response):
            for q in response: 
                if q.url.endswith(queue_name):
                    if callable(callback):
                        callback(q)
        self.get_all_queues(queue_name, callback=queue_got)

    lookup = get_queue

    #
    # Permissions methods
    #

    def add_permission(self, queue, label, aws_account_id, action_name, callback=None):
        """
        Add a permission to a queue.

        :type queue: :class:`boto.sqs.queue.Queue`
        :param queue: The queue object

        :type label: str or unicode
        :param label: A unique identification of the permission you are setting.
                      Maximum of 80 characters ``[0-9a-zA-Z_-]``
                      Example, AliceSendMessage

        :type aws_account_id: str or unicode
        :param principal_id: The AWS account number of the principal who will
                             be given permission.  The principal must have
                             an AWS account, but does not need to be signed
                             up for Amazon SQS. For information
                             about locating the AWS account identification.

        :type action_name: str or unicode
        :param action_name: The action.  Valid choices are:
                            \*|SendMessage|ReceiveMessage|DeleteMessage|
                            ChangeMessageVisibility|GetQueueAttributes

        :rtype: bool
        :return: True if successful, False otherwise.

        """
        params = {'Label': label,
                  'AWSAccountId' : aws_account_id,
                  'ActionName' : action_name}
        return self.get_status('AddPermission', params, queue.id, callback=callback)

    def remove_permission(self, queue, label, callback=None):
        """
        Remove a permission from a queue.

        :type queue: :class:`boto.sqs.queue.Queue`
        :param queue: The queue object

        :type label: str or unicode
        :param label: The unique label associated with the permission
                      being removed.

        :rtype: bool
        :return: True if successful, False otherwise.
        """
        params = {'Label': label}
        return self.get_status('RemovePermission', params, queue.id, callback=callback)

    
    

    
# vim:set ft=python sw=4 :