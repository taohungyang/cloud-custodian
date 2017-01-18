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
from cStringIO import StringIO
import csv
import datetime
from datetime import timedelta
from dateutil.parser import parse
from dateutil.tz import tzutc
import itertools
import time
from botocore.exceptions import ClientError

from c7n.actions import BaseAction
from c7n.filters import ValueFilter, Filter, OPERATORS
from c7n.manager import resources
from c7n.query import QueryResourceManager
from c7n.utils import local_session, type_schema


@resources.register('iam-group')
class Group(QueryResourceManager):

    class resource_type(object):
        service = 'iam'
        type = 'group'
        enum_spec = ('list_groups', 'Groups', None)
        detail_spec = None
        id = 'GroupId'
        name = 'GroupName'
        filter_name = None
        date = 'CreateDate'
        dimension = None
        config_type = "AWS::IAM::Group"


@resources.register('iam-role')
class Role(QueryResourceManager):

    class resource_type(object):
        service = 'iam'
        type = 'role'
        enum_spec = ('list_roles', 'Roles', None)
        detail_spec = None
        id = 'RoleId'
        filter_name = None
        name = 'RoleName'
        date = 'CreateDate'
        dimension = None
        config_type = "AWS::IAM::Role"


@resources.register('iam-user')
class User(QueryResourceManager):

    class resource_type(object):
        service = 'iam'
        type = 'user'
        enum_spec = ('list_users', 'Users', None)
        id = 'UserId'
        filter_name = None
        name = 'UserName'
        date = 'CreateDate'
        dimension = None
        config_type = "AWS::IAM::User"


@resources.register('iam-policy')
class Policy(QueryResourceManager):

    class resource_type(object):
        service = 'iam'
        type = 'policy'
        enum_spec = ('list_policies', 'Policies', None)
        id = 'PolicyId'
        filter_name = None
        name = 'PolicyName'
        date = 'CreateDate'
        dimension = None
        config_type = "AWS::IAM::Policy"


@resources.register('iam-profile')
class InstanceProfile(QueryResourceManager):

    class resource_type(object):
        service = 'iam'
        type = 'instance-profile'
        enum_spec = ('list_instance_profiles', 'InstanceProfiles', None)
        id = 'InstanceProfileId'
        filter_name = None
        name = 'InstanceProfileId'
        date = 'CreateDate'
        dimension = None


@resources.register('iam-certificate')
class ServerCertificate(QueryResourceManager):

    class resource_type(object):
        service = 'iam'
        type = 'server-certificate'
        enum_spec = ('list_server_certificates',
                     'ServerCertificateMetadataList',
                     None)
        id = 'ServerCertificateId'
        filter_name = None
        name = 'ServerCertificateName'
        date = 'Expiration'
        dimension = None


class IamRoleUsage(Filter):

    def get_permissions(self):
        perms = list(itertools.chain([
            self.manager.get_resource_manager(m).get_permissions()
            for m in ['lambda', 'launch-config', 'ec2']]))
        perms.extend(['ecs:DescribeClusters', 'ecs:DescribeServices'])
        return perms

    def service_role_usage(self):
        results = set()
        results.update(self.scan_lambda_roles())
        results.update(self.scan_ecs_roles())
        results.update(self.scan_asg_roles())
        results.update(self.scan_ec2_roles())
        return results

    def instance_profile_usage(self):
        results = set()
        results.update(self.scan_asg_roles())
        results.update(self.scan_ec2_roles())
        return results

    def scan_lambda_roles(self):
        manager = self.manager.get_resource_manager('lambda')
        return [r['Role'] for r in manager.resources() if 'Role' in r]

    def scan_ecs_roles(self):
        results = []
        client = local_session(self.manager.session_factory).client('ecs')
        for cluster in client.describe_clusters()['clusters']:
            svcs = client.list_services(cluster=cluster)['serviceArns']
            for svc in client.describe_services(
                    cluster=cluster, services=svcs)['services']:
                if 'roleArn' not in svc:
                    continue
                results.append(svc['roleArn'])
        return results

    def scan_asg_roles(self):
        manager = self.manager.get_resource_manager('launch-config')
        return [r['IamInstanceProfile'] for r in manager.resources()
                if 'IamInstanceProfile' in r]

    def scan_ec2_roles(self):
        manager = self.manager.get_resource_manager('ec2')
        results = []
        for e in manager.resources():
            if 'Instances' not in e:
                continue
            for i in e['Instances']:
                if 'IamInstanceProfile' not in i:
                    continue
                results.append(i['IamInstanceProfile']['Arn'])
        return results


###################
#    IAM Roles    #
###################


@Role.filter_registry.register('used')
class UsedIamRole(IamRoleUsage):

    schema = type_schema('used')

    def process(self, resources, event=None):
        roles = self.service_role_usage()
        results = []
        for r in resources:
            if r['Arn'] in roles or r['RoleName'] in roles:
                results.append(r)
        self.log.info(
            "%d of %d iam roles currently used.", len(results), len(resources))
        return results


@Role.filter_registry.register('unused')
class UnusedIamRole(IamRoleUsage):

    schema = type_schema('unused')

    def process(self, resources, event=None):
        roles = self.service_role_usage()
        results = []
        for r in resources:
            if r['Arn'] not in roles or r['RoleName'] not in roles:
                results.append(r)
        self.log.info("%d of %d iam roles not currently used.",
                      len(results), len(resources))
        return results


@Role.filter_registry.register('has-inline-policy')
class IamRoleInlinePolicy(Filter):
    """
        Filter IAM roles that have an inline-policy attached

        True: Filter roles that have an inline-policy
        False: Filter roles that do not have an inline-policy
    """

    schema = type_schema('has-inline-policy', value={'type': 'boolean'})
    permissions = ('iam:ListRolePolicies',)

    def _inline_policies(self, client, resource):
        return len(client.list_role_policies(
            RoleName=resource['RoleName'])['PolicyNames'])

    def process(self, resources, event=None):
        c = local_session(self.manager.session_factory).client('iam')
        if self.data.get('value', True):
            return [r for r in resources if self._inline_policies(c, r) > 0]
        return [r for r in resources if self._inline_policies(c, r) == 0]


######################
#    IAM Policies    #
######################


@Policy.filter_registry.register('used')
class UsedIamPolicies(Filter):

    schema = type_schema('used')
    permissions = ('iam:ListPolicies',)

    def process(self, resources, event=None):
        return [r for r in resources if r['AttachmentCount'] > 0]


@Policy.filter_registry.register('unused')
class UnusedIamPolicies(Filter):

    schema = type_schema('unused')
    permissions = ('iam:ListPolicies',)

    def process(self, resources, event=None):
        return [r for r in resources if r['AttachmentCount'] == 0]


###############################
#    IAM Instance Profiles    #
###############################


@InstanceProfile.filter_registry.register('used')
class UsedInstanceProfiles(IamRoleUsage):

    schema = type_schema('used')

    def process(self, resources, event=None):
        results = []
        profiles = self.instance_profile_usage()
        for r in resources:
            if r['Arn'] in profiles or r['InstanceProfileName'] in profiles:
                results.append(r)
        self.log.info(
            "%d of %d instance profiles currently in use." % (
                len(results), len(resources)))
        return results


@InstanceProfile.filter_registry.register('unused')
class UnusedInstanceProfiles(IamRoleUsage):

    schema = type_schema('unused')

    def process(self, resources, event=None):
        results = []
        profiles = self.instance_profile_usage()
        for r in resources:
            if (r['Arn'] not in profiles or
                        r['InstanceProfileName'] not in profiles):
                results.append(r)
        self.log.info(
            "%d of %d instance profiles currently not in use." % (
                len(results), len(resources)))
        return results


###################
#    IAM Users    #
###################

@User.filter_registry.register('credential')
class UserCredentialReport(Filter):
    """Use IAM Credential report to filter users.

    The IAM Credential report ( https://goo.gl/sbEPtM ) aggregates
    multiple pieces of information on iam users. This makes it highly
    efficient for querying multiple aspects of a user that would
    otherwise require per user api calls.

    For example if we wanted to retrieve all users with mfa who have
    never used their password but have active access keys from the
    last month

    .. code-block: yaml

     - name: iam-mfa-active-keys-no-login
       resource: iam-user
       filters:
         - type: credential
           key: mfa_active
           value: true
         - type: credential
           key: password_last_used
           value: absent
         - type: credential
           key: access_keys.last_used
           value_type: age
           value: 30
           op: less-than

    Credential Report Transforms

    We perform some default transformations from the raw
    credential report. Sub-objects (access_key_1, cert_2)
    are turned into array of dictionaries for matching
    purposes with their common prefixes stripped.
    N/A values are turned into None, TRUE/FALSE are turned
    into boolean values.

    """
    schema = type_schema(
        'credential',
        value_type={'type': 'string', 'enum': [
            'age', 'expiration', 'size', 'regex']},

        key={'type': 'string',
             'title': 'report key to search',
             'enum': [
                 'user',
                 'arn',
                 'user_creation_time',
                 'password_enabled',
                 'password_last_used',
                 'password_last_changed',
                 'password_next_rotation',
                 'mfa_active',
                 'access_keys',
                 'access_keys.active',
                 'access_keys.last_used_date',
                 'access_keys.last_used_region',
                 'access_keys.last_used_service',
                 'access_keys.last_rotated',
                 'certs',
                 'certs.active',
                 'certs.last_rotated',
                 ]},
        value={'oneOf': [
            {'type': 'array'},
            {'type': 'string'},
            {'type': 'boolean'},
            {'type': 'number'}]},
        op={'enum': OPERATORS.keys()},
        report_generate={
            'title': 'Generate a report if none is present.',
            'default': True,
            'type': 'boolean'},
        report_delay={
            'title': 'Number of seconds to wait for report generation.',
            'default': 10,
            'type': 'number'},
        report_max_age={
            'title': 'Number of seconds to consider a report valid.',
            'default': 60 * 60 * 24,
            'type': 'number'})

    list_sub_objects = (
        ('access_key_1_', 'access_keys'),
        ('access_key_2_', 'access_keys'),
        ('cert_1_', 'certs'),
        ('cert_2_', 'certs'))

    permissions = ('iam:GenerateCredentialReport',
                   'iam:GetCredentialReport')

    def get_value_or_schema_default(self, k):
        if k in self.data:
            return self.data[k]
        return self.schema['properties'][k]['default']

    def get_credential_report(self):
        report = self.manager._cache.get('iam-credential-report')
        if report:
            return report
        data = self.fetch_credential_report()
        report = {}
        reader = csv.reader(StringIO(data))
        headers = reader.next()
        for line in reader:
            info = dict(zip(headers, line))
            report[info['user']] = self.process_user_record(info)
        self.manager._cache.save('iam-credential-report', report)
        return report

    @classmethod
    def process_user_record(cls, info):
        """Type convert the csv record, modifies in place."""
        keys = info.keys()
        # Value conversion
        for k in keys:
            v = info[k]
            if v in ('N/A', 'no_information'):
                info[k] = None
            elif v == 'false':
                info[k] = False
            elif v == 'true':
                info[k] = True
        # Object conversion
        for p, t in cls.list_sub_objects:
            obj = dict([(k[len(p):], info.pop(k))
                        for k in keys if k.startswith(p)])
            if obj.get('active', False):
                info.setdefault(t, []).append(obj)
        return info

    def fetch_credential_report(self):
        client = local_session(self.manager.session_factory).client('iam')
        try:
            report = client.get_credential_report()
        except ClientError as e:
            if e.response['Error']['Code'] != 'ReportNotPresent':
                raise
            report = None
        if report:
            threshold = datetime.datetime.now(tz=tzutc()) - timedelta(
                seconds=self.get_value_or_schema_default(
                    'report_max_age'))
            if not report['GeneratedTime'].tzinfo:
                threshold = threshold.replace(tzinfo=None)
            if report['GeneratedTime'] < threshold:
                report = None
        if report is None:
            if not self.get_value_or_schema_default('report_generate'):
                raise ValueError("Credential Report Not Present")
            client.generate_credential_report()
            time.sleep(self.get_value_or_schema_default('report_delay'))
            report = client.get_credential_report()
        return report['Content']

    def process(self, resources, event=None):
        if '.' in self.data['key']:
            self.matcher_config = dict(self.data)
            self.matcher_config['key'] = self.data['key'].split('.', 1)[1]
        report = self.get_credential_report()
        if report is None:
            return []
        results = []
        for r in resources:
            info = report.get(r['UserName'])
            if self.match(info):
                r['c7n:credential-report'] = info
                results.append(r)
        return results

    def match(self, info):
        if info is None:
            return False
        k = self.data.get('key')
        if '.' not in k:
            vf = ValueFilter(self.data)
            vf.annotate = False
            return vf(info)

        prefix, sk = k.split('.', 1)
        vf = ValueFilter(self.matcher_config)
        vf.annotate = False
        for v in info.get(prefix, ()):
            if vf.match(v):
                return True


@User.filter_registry.register('policy')
class UserAttachedPolicy(Filter):

    schema = type_schema('policy')
    permissions = ('iam:ListAttachedUserPolicies',)

    def process(self, resources, event=None):

        def _user_policies(resource):
            client = local_session(self.manager.session_factory).client('iam')
            resource['AttachedPolicies'] = client.list_attached_user_policies(
                UserName=resource['UserName'])['AttachedPolicies']

        with self.executor_factory(max_workers=2) as w:
            query_resources = [
                r for r in resources if 'AttachedPolicies' not in r]
            self.log.debug(
                "Querying %d users policies" % len(query_resources))
            list(w.map(_user_policies, query_resources))

        matched = []
        for r in resources:
            for p in r['AttachedPolicies']:
                if self.match(p):
                    matched.append(r)
                    break
        return matched


@User.filter_registry.register('access-key')
class UserAccessKey(ValueFilter):

    schema = type_schema('access-key', rinherit=ValueFilter.schema)
    permissions = ('iam:ListAccessKeys',)

    def process(self, resources, event=None):

        def _user_keys(resource):
            client = local_session(self.manager.session_factory).client('iam')
            resource['AccessKeys'] = client.list_access_keys(
                UserName=resource['UserName'])['AccessKeyMetadata']

        with self.executor_factory(max_workers=2) as w:
            query_resources = [
                r for r in resources if 'AccessKeys' not in r]
            self.log.debug(
                "Querying %d users' api keys" % len(query_resources))
            list(w.map(_user_keys, query_resources))

        matched = []
        for r in resources:
            for p in r['AccessKeys']:
                if self.match(p):
                    matched.append(r)
                    break
        return matched


# Mfa-device filter for iam-users
@User.filter_registry.register('mfa-device')
class UserMfaDevice(ValueFilter):

    schema = type_schema('mfa-device', rinherit=ValueFilter.schema)
    permissions = ('iam:ListMfaDevices',)

    def __init__(self, *args, **kw):
        super(UserMfaDevice, self).__init__(*args, **kw)
        self.data['key'] = 'MFADevices'

    def process(self, resources, event=None):

        def _user_mfa_devices(resource):
            client = local_session(self.manager.session_factory).client('iam')
            resource['MFADevices'] = client.list_mfa_devices(
                UserName=resource['UserName'])['MFADevices']

        with self.executor_factory(max_workers=2) as w:
            query_resources = [
                r for r in resources if 'MFADevices' not in r]
            self.log.debug(
                "Querying %d users' mfa devices" % len(query_resources))
            list(w.map(_user_mfa_devices, query_resources))

        matched = []
        for r in resources:
            if self.match(r):
                matched.append(r)

        return matched


@User.action_registry.register('remove-keys')
class UserRemoveAccessKey(BaseAction):

    schema = type_schema(
        'remove-keys', age={'type': 'number'}, disable={'type': 'boolean'})
    permissions = ('iam:ListAccessKeys', 'iam:UpdateAccessKey',
                   'iam:DeleteAccssKey')

    def process(self, resources):
        client = local_session(self.manager.session_factory).client('iam')

        age = self.data.get('age')
        disable = self.data.get('disable')

        if age:
            threshold_date = datetime.datetime.now(tz=tzutc()) - timedelta(age)

        for r in resources:
            if 'AccessKeys' not in r:
                r['AccessKeys'] = client.list_access_keys(
                    UserName=r['UserName'])['AccessKeyMetadata']
            keys = r['AccessKeys']
            for k in keys:
                if age:
                    if not parse(k['CreateDate']) < threshold_date:
                        continue
                if disable:
                    client.update_access_key(
                        UserName=r['UserName'],
                        AccessKeyId=k['AccessKeyId'],
                        Status='Inactive')
                else:
                    client.delete_access_key(
                        UserName=r['UserName'],
                        AccessKeyId=k['AccessKeyId'])


#################
#   IAM Groups  #
#################


@Group.filter_registry.register('has-users')
class IamGroupUsers(Filter):
    """
        Filter IAM groups that have users attached based on True/False value:

        True: Filter all IAM groups with users assigned to it
        False: Filter all IAM groups without any users assigned to it
    """
    schema = type_schema('has-users', value={'type': 'boolean'})
    permissions = ('iam:GetGroup',)

    def _user_count(self, client, resource):
        return len(client.get_group(GroupName=resource['GroupName'])['Users'])

    def process(self, resources, events=None):
        c = local_session(self.manager.session_factory).client('iam')
        value = self.data.get('value')
        if self.data.get('value', True):
            return [r for r in resources if self._user_count(c, r) > 0]
        return [r for r in resources if self._user_count(c, r) == 0]


@Group.filter_registry.register('has-inline-policy')
class IamGroupInlinePolicy(Filter):
    """
        Filter IAM groups that have an inline-policy based on boolean value:

        True: Filter all groups that have an inline-policy attached
        False: Filter all groups that do not have an inline-policy attached
    """
    schema = type_schema('has-inline-policy', value={'type': 'boolean'})
    permissions = ('iam:ListGroupPolicies',)

    def _inline_policies(self, client, resource):
        return len(client.list_group_policies(
            GroupName=resource['GroupName'])['PolicyNames'])

    def process(self, resources, events=None):
        c = local_session(self.manager.session_factory).client('iam')
        if self.data.get('value', True):
            return [r for r in resources if self._inline_policies(c, r) > 0]
        return [r for r in resources if self._inline_policies(c, r) == 0]
