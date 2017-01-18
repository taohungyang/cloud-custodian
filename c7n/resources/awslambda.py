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
import json
from botocore.exceptions import ClientError

from c7n.actions import BaseAction
from c7n.filters import CrossAccountAccessFilter, ValueFilter
import c7n.filters.vpc as net_filters
from c7n.manager import resources
from c7n.query import QueryResourceManager
from c7n.utils import local_session, type_schema


@resources.register('lambda')
class AWSLambda(QueryResourceManager):

    class resource_type(object):
        service = 'lambda'
        type = 'function'
        enum_spec = ('list_functions', 'Functions', None)
        name = id = 'FunctionName'
        filter_name = None
        date = 'LastModified'
        dimension = 'FunctionName'


@AWSLambda.filter_registry.register('security-group')
class SecurityGroupFilter(net_filters.SecurityGroupFilter):

    RelatedIdsExpression = "VpcConfig.SecurityGroupIds[]"


@AWSLambda.filter_registry.register('subnet')
class SubnetFilter(net_filters.SubnetFilter):

    RelatedIdsExpression = "VpcConfig.SubnetIds[]"


@AWSLambda.filter_registry.register('event-source')
class LambdaEventSource(ValueFilter):
    # this uses iam policy, it should probably use
    # event source mapping api

    annotation_key = "c7n.EventSources"
    schema = type_schema('event-source', rinherit=ValueFilter.schema)
    permissions = ('lambda:GetPolicy',)

    def process(self, resources, event=None):
        def _augment(r):
            if 'c7n.Policy' in r:
                return
            client = local_session(
                self.manager.session_factory).client('lambda')
            try:
                r['c7n.Policy'] = client.get_policy(
                    FunctionName=r['FunctionName'])['Policy']
                return r
            except ClientError as e:
                if e.response['Error']['Code'] == 'AccessDeniedException':
                    self.log.warning(
                        "Access denied getting policy lambda:%s",
                        r['FunctionName'])

        self.log.debug("fetching policy for %d lambdas" % len(resources))
        self.data['key'] = self.annotation_key

        with self.executor_factory(max_workers=3) as w:
            resources = filter(None, w.map(_augment, resources))
            return super(LambdaEventSource, self).process(resources, event)

    def __call__(self, r):
        if 'c7n.Policy' not in r:
            return False
        sources = set()
        data = json.loads(r['c7n.Policy'])
        for s in data.get('Statement', ()):
            if s['Effect'] != 'Allow':
                continue
            if 'Service' in s['Principal']:
                sources.add(s['Principal']['Service'])
            if sources:
                r[self.annotation_key] = list(sources)
        return self.match(r)


@AWSLambda.filter_registry.register('cross-account')
class LambdaCrossAccountAccessFilter(CrossAccountAccessFilter):
    """Filters lambda functions with cross-account permissions

    The whitelist parameter can be used to prevent certain accounts
    from being included in the results (essentially stating that these
    accounts permissions are allowed to exist)

    This can be useful when combining this filter with the delete action.

    :example:

        .. code-block: yaml

            policies:
              - name: lambda-cross-account
                resource: lambda
                filters:
                  - type: cross-account
                    whitelist:
                      - 'IAM-Policy-Cross-Account-Access'

    """
    permissions = ('lambda:GetPolicy',)

    def process(self, resources, event=None):

        def _augment(r):
            client = local_session(
                self.manager.session_factory).client('lambda')
            try:
                r['Policy'] = client.get_policy(
                    FunctionName=r['FunctionName'])['Policy']
                return r
            except ClientError as e:
                if e.response['Error']['Code'] == 'AccessDeniedException':
                    self.log.warning(
                        "Access denied getting policy lambda:%s",
                        r['FunctionName'])

        self.log.debug("fetching policy for %d lambdas" % len(resources))
        with self.executor_factory(max_workers=3) as w:
            resources = filter(None, w.map(_augment, resources))

        return super(LambdaCrossAccountAccessFilter, self).process(
            resources, event)


@AWSLambda.action_registry.register('delete')
class Delete(BaseAction):
    """Delete a lambda function (including aliases and older versions).

    :example:

        .. code-block: yaml

            policies:
              - name: lambda-delete-dotnet-functions
                resource: lambda
                filters:
                  - Runtime: dotnetcore1.0
                actions:
                  - delete
    """
    schema = type_schema('delete')
    permissions = ("lambda:DeleteFunction",)

    def process(self, functions):
        client = local_session(self.manager.session_factory).client('lambda')
        for function in functions:
            try:
                client.delete_function(FunctionName=function['FunctionName'])
            except ClientError as e:
                if e.response['Error']['Code'] == "ResourceNotFoundException":
                    continue
                raise
        self.log.debug("Deleted %d functions", len(functions))
