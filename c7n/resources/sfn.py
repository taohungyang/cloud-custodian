# Copyright 2017 Capital One Services, LLC
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

from c7n.manager import resources
from c7n.query import QueryResourceManager


@resources.register('step-machine')
class StepFunction(QueryResourceManager):
    """AWS Step Functions State Machine"""

    class resource_type(object):
        service = 'stepfunctions'
        enum_spec = ('list_state_machines', 'stateMachines', None)
        id = 'stateMachineArn'
        name = 'name'
        date = 'creationDate'
        dimension = None
        detail_spec = (
            "describe_state_machine", "stateMachineArn",
            'stateMachineArn', None)


