# Copyright 2016 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from c7n.filters import CrossAccountAccessFilter
from c7n.manager import resources
from c7n.query import QueryResourceManager


@resources.register('sns')
class SNS(QueryResourceManager):

    class resource_type(object):
        service = 'sns'
        type = 'topic'
        enum_spec = ('list_topics', 'Topics', None)
        detail_spec = (
            'get_topic_attributes', 'TopicArn', 'TopicArn', 'Attributes')
        id = 'TopicArn'
        filter_name = None
        filter_type = None
        name = 'DisplayName'
        date = None
        dimension = 'TopicName'
        default_report_fields = (
            'TopicArn',
            'DisplayName',
            'SubscriptionsConfirmed',
            'SubscriptionsPending',
            'SubscriptionsDeleted'
        )


@SNS.filter_registry.register('cross-account')
class SNSCrossAccount(CrossAccountAccessFilter):
    permissions = ('sns:GetTopicAttributes',)
