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

from c7n.actions import (
    ActionRegistry, BaseAction
)
from c7n.filters import (
    FilterRegistry, AgeFilter, ValueFilter, Filter, OPERATORS
)

from c7n.query import QueryResourceManager
from c7n.manager import resources

from c7n import utils
from c7n.utils import type_schema, local_session

filters = FilterRegistry('health.actions')
actions = ActionRegistry('health.filters')


@resources.register('health-events')
class HealthEvents(QueryResourceManager):
    """Query resource manager for AWS health events"""

    class resource_type(object):
        service = 'health'
        type = 'event'
        enum_spec = ('describe_events', 'events', None)
        name = 'eventTypeCode'
        id = 'arn'
        filter_name = None
        filter_type = None
        dimension = None
        date = 'startTime'
        config_type = 'AWS::Health::Event'

    filter_registry = filters
    action_registry = actions

    def __init__(self, ctx, data):
        super(HealthEvents, self).__init__(ctx, data)
        self.queries = QueryFilter.parse(
            self.data.get('query', [{'eventStatusCodes': 'open'}]))

    def get_model(self):
        return self.resource_type

    def resource_query(self):
        qf = {}
        for q in self.queries:
            qd = q.query()
            print qd
            if qd['Name'] in qf:
                for qv in qf[qd['Name']]:
                    if qv in qf[qd['Name']]:
                        continue
                    qf[qd['Name']].append(qv)
            else:
                qf[qd['Name']] = []
                for qv in qd['Values']:
                    qf[qd['Name']].append(qv)
        return qf

    def resources(self, query=None):
        q = self.resource_query()
        if q is not None:
            query = query or {}
            query['filter'] = q
        return super(HealthEvents, self).resources(query=query)

    def augment(self, resources):
        # # paginator, faster
        # if len(resources) > 0:
        #     client = local_session(self.session_factory).client('health')
        #     paginator = client.get_paginator('describe_affected_entities')
        #     events_map = {e['arn']: e for e in resources}

        #     for p in paginator.paginate(filter={'eventArns': events_map.keys()}):
        #         for e in p['entities']:
        #             events_map[e['eventArn']]['affectedEntities'] = e

        # works but takes longer time
        client = local_session(self.session_factory).client('health')
        for r in resources:
            affectedEntities = client.describe_affected_entities(filter={'eventArns':[r['arn']]})['entities']
            del affectedEntities[0]['eventArn']
            if affectedEntities[0].get('awsAccountId'):
                del affectedEntities[0]['awsAccountId']
            r['affectedEntities'] = affectedEntities
            r['eventDescription'] = client.describe_event_details(eventArns=[r['arn']])['successfulSet'][0]['eventDescription']

        return resources

HEALTH_VALID_FILTERS = {
    'availability-zone': str,
    'eventTypeCategory': {'issue', 'accountNotification', 'scheduledChange'},
    'regions': str,
    'services': str,
    'eventStatusCodes': {'open', 'closed', 'upcoming'},
    'eventTypeCodes': str
}


class QueryFilter(object):

    @classmethod
    def parse(cls, data):
        results = []
        for d in data:
            if not isinstance(d, dict):
                raise ValueError(
                    "Health Query Filter Invalid structure %s" % d)
            results.append(cls(d).validate())
        return results

    def __init__(self, data):
        self.data = data
        self.key = None
        self.value = None

    def validate(self):
        if not len(self.data.keys()) == 1:
            raise ValueError(
                "Health Query Filter Invalid %s" % self.data)
        self.key = self.data.keys()[0]
        self.value = self.data.values()[0]

        if self.key not in HEALTH_VALID_FILTERS:
            raise ValueError(
                "Health Query Filter invalid filter name %s" % (self.data))

        if self.value is None:
            raise ValueError(
                "Health Query Filters must have a value, use tag-key"
                " w/ tag name as value for tag present checks"
                " %s" % self.data)
        return self

    def query(self):
        value = self.value
        if isinstance(self.value, basestring):
            value = [self.value]
        return {'Name': self.key, 'Values': value}