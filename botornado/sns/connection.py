import uuid
import hashlib

from boto.connection import AWSQueryConnection
from boto.regioninfo import RegionInfo
from boto.compat import json
import boto
import boto.sns

import botornado.connection
import botornado.sqs.queue
import botornado.sqs.message


class AsyncSNSConnection(botornado.connection.AsyncAWSQueryConnection, boto.sns.SNSConnection):
    __doc__ = boto.sns.SNSConnection.__doc__
    DefaultRegionName = boto.config.get('Boto', 'sns_region_name', 'us-east-1')
    DefaultRegionEndpoint = boto.config.get('Boto', 'sns_region_endpoint', 
                                            'sns.us-east-1.amazonaws.com')
    APIVersion = boto.config.get('Boto', 'sns_version', '2010-03-31')


    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None, debug=0,
                 https_connection_factory=None, region=None, path='/',
                 security_token=None, validate_certs=True,
                 profile_name=None):
        if not region:
            region = RegionInfo(self, self.DefaultRegionName,
                                self.DefaultRegionEndpoint,
                                connection_cls=AsyncSNSConnection)
        self.region = region
        botornado.connection.AsyncAWSQueryConnection.__init__(self,
                                    aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key,
                                    is_secure=is_secure,
                                    port=port,
                                    proxy=proxy,
                                    proxy_port=proxy_port,
                                    proxy_user=proxy_user,
                                    proxy_pass=proxy_pass,
                                    host=self.region.endpoint,
                                    debug=debug,
                                    https_connection_factory=https_connection_factory,
                                    path=path,
                                    security_token=security_token,
                                    validate_certs=validate_certs,
                                    profile_name=profile_name)

    def get_all_topics(self, next_token=None, callback=None):
        __doc__ = boto.sns.SNSConnection.get_all_topics.__doc__
        params = {}
        if next_token:
            params['NextToken'] = next_token
        return self._make_request('ListTopics', params, callback=callback)

    def get_topic_attributes(self, topic, callback=None):
        __doc__ = boto.sns.SNSConnection.get_topic_attributes.__doc__
        params = {'TopicArn': topic}
        return self._make_request('GetTopicAttributes', params, callback=callback)

    def set_topic_attributes(self, topic, attr_name, attr_value, callback=None):
        __doc__ = boto.sns.SNSConnection.set_topic_attributes.__doc__
        params = {'TopicArn': topic,
                  'AttributeName': attr_name,
                  'AttributeValue': attr_value}
        return self._make_request('SetTopicAttributes', params, callback=callback)

    def add_permission(self, topic, label, account_ids, actions, callback=None):
        __doc__ = boto.sns.SNSConnection.add_permission.__doc__
        params = {'TopicArn': topic,
                  'Label': label}
        self.build_list_params(params, account_ids, 'AWSAccountId.member')
        self.build_list_params(params, actions, 'ActionName.member')
        return self._make_request('AddPermission', params, callback=callback)

    def remove_permission(self, topic, label, callback=None):
        __doc__ = boto.sns.SNSConnection.remove_permission.__doc__
        params = {'TopicArn': topic,
                  'Label': label}
        return self._make_request('RemovePermission', params, callback=callback)

    def create_topic(self, topic, callback=None):
        __doc__ = boto.sns.SNSConnection.create_topic.__doc__
        params = {'Name': topic}
        return self._make_request('CreateTopic', params, callback=callback)

    def delete_topic(self, topic, callback=None):
        __doc__ = boto.sns.SNSConnection.delete_topic.__doc__
        params = {'TopicArn': topic}
        return self._make_request('DeleteTopic', params, '/', 'GET', callback=callback)

    def publish(self, topic=None, message=None, subject=None, target_arn=None,
                message_structure=None, callback=None):
        __doc__ = boto.sns.SNSConnection.delete_topic.__doc__
        if message is None:
            # To be backwards compatible when message did not have
            # a default value and topic and message were required
            # args.
            raise TypeError("'message' is a required parameter")
        params = {'Message': message}
        if subject is not None:
            params['Subject'] = subject
        if topic is not None:
            params['TopicArn'] = topic
        if target_arn is not None:
            params['TargetArn'] = target_arn
        if message_structure is not None:
            params['MessageStructure'] = message_structure
        return self._make_request('Publish', params, '/', 'POST', callback=callback)

    def subscribe(self, topic, protocol, endpoint, callback=None):
        __doc__ = boto.sns.SNSConnection.subscribe.__doc__
        params = {'TopicArn': topic,
                  'Protocol': protocol,
                  'Endpoint': endpoint}
        return self._make_request('Subscribe', params, callback=callback)

    def subscribe_sqs_queue(self, topic, queue, callback=None):
        raise NotImplemented()

    def confirm_subscription(self, topic, token,
                             authenticate_on_unsubscribe=False):
        """
        Get properties of a Topic

        :type topic: string
        :param topic: The ARN of the new topic.

        :type token: string
        :param token: Short-lived token sent to and endpoint during
                      the Subscribe operation.

        :type authenticate_on_unsubscribe: bool
        :param authenticate_on_unsubscribe: Optional parameter indicating
                                            that you wish to disable
                                            unauthenticated unsubscription
                                            of the subscription.

        """
        params = {'TopicArn': topic, 'Token': token}
        if authenticate_on_unsubscribe:
            params['AuthenticateOnUnsubscribe'] = 'true'
        return self._make_request('ConfirmSubscription', params)

    def unsubscribe(self, subscription, callback=None):
        __doc__ = boto.sns.SNSConnection.unsubscribe.__doc__
        params = {'SubscriptionArn': subscription}
        return self._make_request('Unsubscribe', params, callback=callback)

    def get_all_subscriptions(self, next_token=None, callback=None):
        __doc__ = boto.sns.SNSConnection.get_all_subscriptions.__doc__
        params = {}
        if next_token:
            params['NextToken'] = next_token
        return self._make_request('ListSubscriptions', params, callback=callback)

    def get_all_subscriptions_by_topic(self, topic, next_token=None, callback=None):
        __doc__ = boto.sns.SNSConnection.get_all_subscriptions_by_topic.__doc__
        params = {'TopicArn': topic}
        if next_token:
            params['NextToken'] = next_token
        return self._make_request('ListSubscriptionsByTopic', params, callback=callback)

    def create_platform_application(self, name=None, platform=None,
                                    attributes=None, callback=None):
        __doc__ = boto.sns.SNSConnection.create_platform_application.__doc__
        params = {}
        if name is not None:
            params['Name'] = name
        if platform is not None:
            params['Platform'] = platform
        if attributes is not None:
            self._build_dict_as_list_params(params, attributes, 'Attributes')
        return self._make_request(action='CreatePlatformApplication',
                                  params=params, callback=callback)

    def set_platform_application_attributes(self,
                                            platform_application_arn=None,
                                            attributes=None, callback=None):
        __doc__ = boto.sns.SNSConnection.set_platform_application_attributes.__doc__
        params = {}
        if platform_application_arn is not None:
            params['PlatformApplicationArn'] = platform_application_arn
        if attributes is not None:
            self._build_dict_as_list_params(params, attributes, 'Attributes')
        return self._make_request(action='SetPlatformApplicationAttributes',
                                  params=params, callback=callback)

    def get_platform_application_attributes(self,
                                            platform_application_arn=None, callback=None):
        __doc__ = boto.sns.SNSConnection.get_platform_application_attributes.__doc__
        params = {}
        if platform_application_arn is not None:
            params['PlatformApplicationArn'] = platform_application_arn
        return self._make_request(action='GetPlatformApplicationAttributes',
                                  params=params, callback=callback)

    def list_platform_applications(self, next_token=None, callback=None):
        __doc__ = boto.sns.SNSConnection.list_platform_applications.__doc__
        params = {}
        if next_token is not None:
            params['NextToken'] = next_token
        return self._make_request(action='ListPlatformApplications',
                                  params=params, callback=callback)

    def list_endpoints_by_platform_application(self,
                                               platform_application_arn=None,
                                               next_token=None, callback=None):
        __doc__ = boto.sns.SNSConnection.list_endpoints_by_platform_application.__doc__
        params = {}
        if platform_application_arn is not None:
            params['PlatformApplicationArn'] = platform_application_arn
        if next_token is not None:
            params['NextToken'] = next_token
        return self._make_request(action='ListEndpointsByPlatformApplication',
                                  params=params, callback=callback)

    def delete_platform_application(self, platform_application_arn=None, callback=None):
        __doc__ = boto.sns.SNSConnection.delete_platform_application.__doc__
        params = {}
        if platform_application_arn is not None:
            params['PlatformApplicationArn'] = platform_application_arn
        return self._make_request(action='DeletePlatformApplication',
                                  params=params, callback=callback)

    def create_platform_endpoint(self, platform_application_arn=None,
                                 token=None, custom_user_data=None,
                                 attributes=None, callback=None):
        __doc__ = boto.sns.SNSConnection.create_platform_endpoint.__doc__
        params = {}
        if platform_application_arn is not None:
            params['PlatformApplicationArn'] = platform_application_arn
        if token is not None:
            params['Token'] = token
        if custom_user_data is not None:
            params['CustomUserData'] = custom_user_data
        if attributes is not None:
            self._build_dict_as_list_params(params, attributes, 'Attributes')
        return self._make_request(action='CreatePlatformEndpoint',
                                  params=params, callback=callback)

    def delete_endpoint(self, endpoint_arn=None, callback=None):
        __doc__ = boto.sns.SNSConnection.delete_endpoint.__doc__
        params = {}
        if endpoint_arn is not None:
            params['EndpointArn'] = endpoint_arn
        return self._make_request(action='DeleteEndpoint', params=params, callback=callback)

    def set_endpoint_attributes(self, endpoint_arn=None, attributes=None, callback=None):
        __doc__ = boto.sns.SNSConnection.set_endpoint_attributes.__doc__
        params = {}
        if endpoint_arn is not None:
            params['EndpointArn'] = endpoint_arn
        if attributes is not None:
            self._build_dict_as_list_params(params, attributes, 'Attributes')
        return self._make_request(action='SetEndpointAttributes',
                                  params=params, callback=callback)

    def get_endpoint_attributes(self, endpoint_arn=None, callback=None):
        __doc__ = boto.sns.SNSConnection.get_endpoint_attributes.__doc__
        params = {}
        if endpoint_arn is not None:
            params['EndpointArn'] = endpoint_arn
        return self._make_request(action='GetEndpointAttributes',
                                  params=params, callback=callback)

    def _make_request(self, action, params, path='/', verb='GET', callback=None):
        params['ContentType'] = 'JSON'

        def request_made(response):
            body = response.read()
            boto.log.debug(body)
            if response.status == 200:
                if callable(callback):
                    callback(json.loads(body))
            else:
                boto.log.error('%s %s' % (response.status, response.reason))
                boto.log.error('%s' % body)
                raise self.ResponseError(response.status, response.reason, body)

        response = self.make_request(action=action, verb=verb,
                                     path=path, params=params, callback=callback)
