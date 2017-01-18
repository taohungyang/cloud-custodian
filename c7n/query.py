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
"""
Query capability built on skew metamodel

tags_spec -> s3, elb, rds
"""
import functools
import itertools
import jmespath

from botocore.client import ClientError

from c7n.actions import ActionRegistry
from c7n.filters import FilterRegistry, MetricsFilter
from c7n.tags import register_tags
from c7n.utils import local_session, get_retry, chunks
from c7n.manager import ResourceManager


class ResourceQuery(object):

    def __init__(self, session_factory):
        self.session_factory = session_factory

    @staticmethod
    def resolve(resource_type):
        if not isinstance(resource_type, type):
            raise ValueError(resource_type)
        else:
            m = resource_type
        return m

    def filter(self, resource_type, **params):
        """Query a set of resources."""
        m = self.resolve(resource_type)
        client = local_session(self.session_factory).client(
            m.service)
        enum_op, path, extra_args = m.enum_spec
        if extra_args:
            params.update(extra_args)

        if client.can_paginate(enum_op):
            p = client.get_paginator(enum_op)
            results = p.paginate(**params)
            data = results.build_full_result()
        else:
            op = getattr(client, enum_op)
            data = op(**params)
        if path:
            path = jmespath.compile(path)
            data = path.search(data)
        if data is None:
            data = []
        return data

    def get(self, resource_type, identities):
        """Get resources by identities
        """
        m = self.resolve(resource_type)
        params = {}
        client_filter = False

        # Try to formulate server side query
        if m.filter_name:
            if m.filter_type == 'list':
                params[m.filter_name] = identities
            elif m.filter_type == 'scalar':
                assert len(identities) == 1, "Scalar server side filter"
                params[m.filter_name] = identities[0]
        else:
            client_filter = True

        resources = self.filter(resource_type, **params)
        if client_filter:
            resources = [r for r in resources if r[m.id] in identities]

        return resources


class QueryMeta(type):

    def __new__(cls, name, parents, attrs):
        if 'filter_registry' not in attrs:
            attrs['filter_registry'] = FilterRegistry(
                '%s.filters' % name.lower())
        if 'action_registry' not in attrs:
            attrs['action_registry'] = ActionRegistry(
                '%s.filters' % name.lower())

        if attrs['resource_type']:
            m = ResourceQuery.resolve(attrs['resource_type'])
            # Generic cloud watch metrics support
            if m.dimension and 'metrics':
                attrs['filter_registry'].register('metrics', MetricsFilter)
            # EC2 Service boilerplate ...
            if m.service == 'ec2':
                # Generic ec2 retry
                attrs['retry'] = staticmethod(get_retry((
                    'RequestLimitExceeded', 'Client.RequestLimitExceeded')))
                # Generic ec2 resource tag support
                if getattr(m, 'taggable', True):
                    register_tags(
                        attrs['filter_registry'], attrs['action_registry'])
        return super(QueryMeta, cls).__new__(cls, name, parents, attrs)


def _napi(op_name):
    return op_name.title().replace('_', '')


class QueryResourceManager(ResourceManager):

    __metaclass__ = QueryMeta

    resource_type = ""
    id_field = ""
    report_fields = []
    retry = None
    max_workers = 3
    chunk_size = 20
    permissions = ()

    def __init__(self, data, options):
        super(QueryResourceManager, self).__init__(data, options)
        self.query = ResourceQuery(self.session_factory)

    @classmethod
    def get_model(cls):
        return ResourceQuery.resolve(cls.resource_type)

    @classmethod
    def match_ids(cls, ids):
        """return ids that match this resource type's id format."""
        id_prefix = getattr(cls.get_model(), 'id_prefix', None)
        if id_prefix is not None:
            return [i for i in ids if i.startswith(id_prefix)]
        return ids

    @classmethod
    def get_permissions(cls):
        perms = []
        m = cls.get_model()
        perms.append('%s:%s' % (m.service, _napi(m.enum_spec[0])))
        if getattr(m, 'detail_spec', None):
            perms.append("%s:%s" % (m.service, _napi(m.detail_spec[0])))
        if getattr(cls, 'permissions', None):
            perms.extend(cls.permissions)
        return perms

    def resources(self, query=None):
        key = {'region': self.config.region,
               'resource': str(self.__class__.__name__),
               'q': query}

        if self._cache.load():
            resources = self._cache.get(key)
            if resources is not None:
                self.log.debug("Using cached %s: %d" % (
                    "%s.%s" % (
                        self.__class__.__module__,
                        self.__class__.__name__),
                    len(resources)))
                return self.filter_resources(resources)

        if query is None:
            query = {}

        if self.retry:
            resources = self.retry(
                self.query.filter, self.resource_type, **query)
        else:
            resources = self.query.filter(self.resource_type, **query)
        resources = self.augment(resources)
        self._cache.save(key, resources)
        return self.filter_resources(resources)

    def get_resources(self, ids, cache=True):
        key = {'region': self.config.region,
               'resource': str(self.__class__.__name__),
               'q': None}
        if cache and self._cache.load():
            resources = self._cache.get(key)
            if resources is not None:
                self.log.debug("Using cached results for get_resources")
                m = self.get_model()
                id_set = set(ids)
                return [r for r in resources if r[m.id] in id_set]
        try:
            resources = self.query.get(self.resource_type, ids)
            resources = self.augment(resources)
            return resources
        except ClientError as e:
            self.log.warning("event ids not resolved: %s error:%s" % (ids, e))
            return []

    def augment(self, resources):
        """subclasses may want to augment resources with additional information.

        ie. we want tags by default (rds, elb), and policy, location, acl for
        s3 buckets.
        """
        model = self.get_model()
        if getattr(model, 'detail_spec', None):
            detail_spec = getattr(model, 'detail_spec', None)
            _augment = _scalar_augment
        elif getattr(model, 'batch_detail_spec', None):
            detail_spec = getattr(model, 'batch_detail_spec', None)
            _augment = _batch_augment
        else:
            return resources
        _augment = functools.partial(_augment, self, model, detail_spec)
        with self.executor_factory(max_workers=self.max_workers) as w:
            results = list(w.map(_augment, chunks(resources, self.chunk_size)))
            return list(itertools.chain(*results))


def _batch_augment(manager, model, detail_spec, resource_set):
    detail_op, param_name, param_key, detail_path = detail_spec
    client = local_session(manager.session_factory).client(model.service)
    op = getattr(client, detail_op)
    if manager.retry:
        args = (op,)
        op = manager.retry
    else:
        args = ()
    kw = {param_name: [param_key and r[param_key] or r for r in resource_set]}
    response = op(*args, **kw)
    return response[detail_path]


def _scalar_augment(manager, model, detail_spec, resource_set):
    detail_op, param_name, param_key, detail_path = detail_spec
    client = local_session(manager.session_factory).client(model.service)
    op = getattr(client, detail_op)
    if manager.retry:
        args = (op,)
        op = manager.retry
    else:
        args = ()
    results = []
    for r in resource_set:
        kw = {param_name: param_key and r[param_key] or r}
        response = op(*args, **kw)
        if detail_path:
            response = response[detail_path]
        else:
            response.pop('ResponseMetadata')
        if param_key is None:
            response[model.id] = r
            r = response
        else:
            r.update(response)
        results.append(r)
    return results

