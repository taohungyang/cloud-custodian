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
import logging

from botocore.exceptions import ClientError

from common import BaseTest
from c7n.resources.ebs import (
    CopyInstanceTags, EncryptInstanceVolumes, CopySnapshot, Delete)
from c7n.executor import MainThreadExecutor


logging.basicConfig(level=logging.DEBUG)


class SnapshotAccessTest(BaseTest):

    def test_snapshot_access(self):
        # pre conditions, 2 snapshots one shared to a separate account, and one
        # shared publicly. 2 non matching volumes, one not shared, one shared
        # explicitly to its own account.
        self.patch(CopySnapshot, 'executor_factory', MainThreadExecutor)
        factory = self.replay_flight_data('test_ebs_cross_account')
        p = self.load_policy({
            'name': 'snap-copy',
            'resource': 'ebs-snapshot',
            'filters': ['cross-account'],
            }, session_factory=factory)
        resources = p.run()
        self.assertEqual(len(resources), 2)
        self.assertEqual(
            {r['SnapshotId']: r['c7n:CrossAccountViolations']
             for r in resources},
            {'snap-7f9496cf': ['619193117841'],
             'snap-af0eb71b': ['all']})


class SnapshotCopyTest(BaseTest):

    def test_snapshot_copy(self):
        self.patch(CopySnapshot, 'executor_factory', MainThreadExecutor)
        self.change_environment(AWS_DEFAULT_REGION='us-west-2')

        factory = self.replay_flight_data('test_ebs_snapshot_copy')
        p = self.load_policy({
            'name': 'snap-copy',
            'resource': 'ebs-snapshot',
            'filters': [
                {'tag:ASV': 'RoadKill'}],
            'actions': [
                {'type': 'copy',
                 'target_region': 'us-east-1',
                 'target_key': '82645407-2faa-4d93-be71-7d6a8d59a5fc'}]
            }, session_factory=factory)
        resources = p.run()

        self.assertEqual(len(resources), 1)
        client = factory(region="us-east-1").client('ec2')
        tags = client.describe_tags(
            Filters=[{'Name': 'resource-id',
                      'Values': [resources[0][
                          'c7n:CopiedSnapshot']]}])['Tags']
        tags = {t['Key']: t['Value'] for t in tags}
        self.assertEqual(tags['ASV'], 'RoadKill')


class SnapshotAmiSnapshotTest(BaseTest):

    def test_snapshot_ami_snapshot_filter(self):
        self.patch(CopySnapshot, 'executor_factory', MainThreadExecutor)
        # DEFAULT_REGION needs to be set to west for recording
        factory = self.replay_flight_data('test_ebs_ami_snapshot_filter')

        #first case should return only resources that are ami snapshots
        p = self.load_policy({
            'name': 'ami-snap-filter',
            'resource': 'ebs-snapshot',
            'filters': [
                {'type': 'skip-ami-snapshots',
                 'value': False}],
            }, session_factory=factory)
        resources = p.run()
        self.assertEqual(len(resources), 3)

        #second case should return resources that are NOT ami snapshots
        policy = self.load_policy({
            'name': 'non-ami-snap-filter',
            'resource': 'ebs-snapshot',
            'filters': [
                {'type': 'skip-ami-snapshots',
                 'value': True}],
            }, session_factory=factory)
        resources = policy.run()
        self.assertEqual(len(resources), 2)


class SnapshotTrimTest(BaseTest):

    def test_snapshot_trim(self):
        factory = self.replay_flight_data('test_ebs_snapshot_delete')
        p = self.load_policy({
            'name': 'snapshot-trim',
            'resource': 'ebs-snapshot',
            'filters': [
                {'tag:InstanceId': 'not-null'}],
            'actions': ['delete']},
            session_factory=factory)
        resources = p.run()
        self.assertEqual(len(resources), 1)


class AttachedInstanceTest(BaseTest):

    def test_ebs_instance_filter(self):
        factory = self.replay_flight_data('test_ebs_instance_filter')
        p = self.load_policy({
            'name': 'attached-instance-test',
            'resource': 'ebs',
            'filters': [
                {'type': 'instance',
                 'key': 'tag:Name',
                 'value': 'CompiledLambda'}]},
            session_factory=factory)
        resources = p.run()
        self.assertEqual(len(resources), 1)


class CopyInstanceTagsTest(BaseTest):

    def test_copy_instance_tags(self):
        # More a functional/coverage test then a unit test.
        self.patch(
            CopyInstanceTags, 'executor_factory', MainThreadExecutor)
        factory = self.replay_flight_data('test_ebs_copy_instance_tags')

        volume_id = 'vol-2b047792'

        results = factory().client('ec2').describe_tags(
            Filters=[{'Name': 'resource-id', 'Values': [volume_id]}])['Tags']
        tags = {t['Key']: t['Value'] for t in results}
        self.assertEqual(tags, {})

        policy = self.load_policy({
            'name': 'test-copy-instance-tags',
            'resource': 'ebs',
            'actions': [{
                'type': 'copy-instance-tags',
                'tags': ['Name']}]},
            config={'region': 'us-west-2'},
            session_factory=factory)

        resources = policy.run()
        results = factory().client('ec2').describe_tags(
            Filters=[{'Name': 'resource-id', 'Values': [volume_id]}])['Tags']

        tags = {t['Key']: t['Value'] for t in results}
        self.assertEqual(tags['Name'], 'CompileLambda')


class VolumeSnapshotTest(BaseTest):

    def test_volume_snapshot(self):
        factory = self.replay_flight_data('test_ebs_snapshot')
        policy = self.load_policy(
            {
                'name': 'test-ebs-snapshot',
                'resource': 'ebs',
                'filters': [{'VolumeId': 'vol-01adbb6a4f175941d'}],
                'actions': ['snapshot'],
            },
            session_factory=factory,
        )
        resources = policy.run()
        snapshot_data = factory().client('ec2').describe_snapshots(
            Filters=[
                {
                    'Name': 'volume-id',
                    'Values': ['vol-01adbb6a4f175941d'],
                },
            ]
        )
        self.assertEqual(len(snapshot_data['Snapshots']), 1)
        

class VolumeDeleteTest(BaseTest):

    def test_volume_delete_force(self):
        self.patch(Delete, 'executor_factory', MainThreadExecutor)
        factory = self.replay_flight_data('test_ebs_force_delete')
        policy = self.load_policy({
            'name': 'test-ebs',
            'resource': 'ebs',
            'filters': [{'VolumeId': 'vol-d0790258'}],
            'actions': [
                {'type': 'delete', 'force': True}]},
            session_factory=factory)
        resources = policy.run()

        try:
            results = factory().client('ec2').describe_volumes(
                VolumeIds=[resources[0]['VolumeId']])
        except ClientError as e:
            self.assertEqual(
                e.response['Error']['Code'], 'InvalidVolume.NotFound')
        else:
            self.fail("Volume still exists")


class EncryptExtantVolumesTest(BaseTest):

    def test_encrypt_volumes(self):
        self.patch(
            EncryptInstanceVolumes, 'executor_factory', MainThreadExecutor)
        output = self.capture_logging(level=logging.DEBUG)

        session_factory = self.replay_flight_data('test_encrypt_volumes')

        policy = self.load_policy({
            'name': 'ebs-remediate-attached',
            'resource': 'ebs',
            'filters': [
                {'Encrypted': False},
                {'VolumeId': 'vol-fdd1f844'}],
            'actions': [
                {'type': 'encrypt-instance-volumes',
                 'delay': 0.1,
                 'key': 'alias/ebs/crypto'}]},
            session_factory=session_factory)
        resources = policy.run()
        self.assertEqual(len(resources), 1)
        self.assertEqual(
            resources[0]['Encrypted'], False)


class TestKmsAlias(BaseTest):

    def test_ebs_kms_alias(self):
        session_factory = self.replay_flight_data('test_ebs_aws_managed_kms_keys')
        p = self.load_policy(
            {'name': 'ebs-aws-managed-kms-keys-filters',
             'resource': 'ebs',
             'filters': [
                 {'type': 'kms-alias', 'key': 'AliasName',
                  'value': '^(alias/aws/)', 'op': 'regex'}]},
            config={'region': 'us-west-2'},
            session_factory=session_factory)

        resources = p.run()
        self.assertEqual(len(resources), 1)
        self.assertEqual(resources[0]['VolumeId'], 'vol-14a3cd9d')


class EbsFaultToleranceTest(BaseTest):

    def test_ebs_fault_tolerant(self):
        session = self.replay_flight_data('test_ebs_fault_tolerant')
        policy = self.load_policy({
            'name': 'ebs-fault-tolerant',
            'resource': 'ebs',
            'filters': ['fault-tolerant']
        }, session_factory=session)
        resources = policy.run()
        self.assertEqual(len(resources), 1)
        self.assertEqual(resources[0]['VolumeId'], 'vol-c5eaa459')

    def test_ebs_non_fault_tolerant(self):
        session = self.replay_flight_data('test_ebs_non_fault_tolerant')
        policy = self.load_policy({
            'name': 'ebs-non-fault-tolerant',
            'resource': 'ebs',
            'filters': [{
                'type': 'fault-tolerant',
                'tolerant': False}]
        }, session_factory=session)
        resources = policy.run()
        self.assertEqual(len(resources), 1)
        self.assertEqual(resources[0]['VolumeId'], 'vol-abdb8d37')

class PiopsMetricsFilterTest(BaseTest):

    def test_ebs_metrics_percent_filter(self):
        session = self.replay_flight_data('test_ebs_metrics_percent_filter')
        policy = self.load_policy({
            'name': 'ebs-unused-piops',
            'resource': 'ebs',
            'filters': [{
                'type': 'metrics',
                'name': 'VolumeConsumedReadWriteOps',
                'op': 'lt',
                'value': 50,
                'statistics': 'Maximum',
                'days': 1,
                'percent-attr': 'Iops'}]
            }, session_factory=session)
        resources = policy.run()
        self.assertEqual(len(resources),1)


class HealthEventsFilterTest(BaseTest):
    def test_ebs_health_events_filter(self):
        session_factory = self.replay_flight_data(
            'test_ebs_health_events_filter')
        policy = self.load_policy({
            'name': 'ebs-health-events-filter',
            'resource': 'ebs',
            'filters': [{
                'type': 'health-event',
                'types': ['AWS_EBS_VOLUME_LOST']}]
                }, session_factory=session_factory)
        resources = policy.run()
        self.assertEqual(len(resources), 1)
        for r in resources:
            self.assertTrue(('c7n:HealthEvent' in r) and
                            ('Description' in e for e in r['c7n:HealthEvent']))
