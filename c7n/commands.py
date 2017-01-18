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
from __future__ import print_function

from datetime import timedelta, datetime
from functools import wraps
import inspect
import json
import logging
import os
import pprint
import sys
import time

import yaml

from c7n.policy import Policy, load as policy_load
from c7n.reports import report as do_report
from c7n.utils import Bag, dumps
from c7n.manager import resources
from c7n.resources import load_resources
from c7n import schema


log = logging.getLogger('custodian.commands')


def policy_command(f):

    @wraps(f)
    def _load_policies(options):
        load_resources()
        collection = policy_load(options, options.config)
        policies = collection.filter(options.policy_filter)
        
        if options.policy_filter and len(policies) == 0 and len(collection) > 0:
            eprint("Warning: no policies matched the filter ({})".format(options.policy_filter))
            eprint("  Policies:")
            for policy in collection.policies():
                eprint("    - ", policy.name)
            eprint()

        return f(options, policies)

    return _load_policies


def validate(options):
    load_resources()
    if options.config is not None:
        # support the old -c option
        options.configs.append(options.config)
    if len(options.configs) < 1:
        # no configs to test
        # We don't have the parser object, so fake ArgumentParser.error
        eprint('Error: no config files specified')
        sys.exit(1)
    used_policy_names = set()
    schm = schema.generate()
    errors = []
    for config_file in options.configs:
        config_file = os.path.expanduser(config_file)
        if not os.path.exists(config_file):
            raise ValueError("Invalid path for config %r" % config_file)

        options.dryrun = True
        format = config_file.rsplit('.', 1)[-1]
        with open(config_file) as fh:
            if format in ('yml', 'yaml'):
                data = yaml.safe_load(fh.read())
            if format in ('json',):
                data = json.load(fh)

        errors = schema.validate(data, schm)
        conf_policy_names = {p['name'] for p in data.get('policies', ())}
        dupes = conf_policy_names.intersection(used_policy_names)
        if len(dupes) >= 1:
            errors.append(ValueError(
                "Only one policy with a given name allowed, duplicates: %s" % (
                    ", ".join(dupes)
                )
            ))
        used_policy_names = used_policy_names.union(conf_policy_names)
        if not errors:
            null_config = Bag(dryrun=True, log_group=None, cache=None, assume_role="na")
            for p in data.get('policies', ()):
                try:
                    Policy(p, null_config, Bag())
                except Exception as e:
                    msg = "Policy: %s is invalid: %s" % (
                        p.get('name', 'unknown'), e)
                    errors.append(msg)
        if not errors:
            log.info("Configuration valid: {}".format(config_file))
            continue

        log.error("Configuration invalid: {}".format(config_file))
        for e in errors:
            log.error(" %s" % e)
    if errors:
        sys.exit(1)


@policy_command
def access(options, policies):
    permissions = set()
    for p in policies:
        permissions.update(p.get_permissions())
    pprint.pprint(sorted(list(permissions)))


@policy_command
def run(options, policies):
    exit_code = 0
    for policy in policies:
        try:
            policy()
        except Exception:
            exit_code = 2
            if options.debug:
                raise
            log.exception(
                "Error while executing policy %s, continuing" % (
                    policy.name))
    sys.exit(exit_code)


@policy_command
def report(options, policies):
    if len(policies) == 0:
        eprint("Error: No policy supplied")
        sys.exit(1)

    if len(policies) > 1:
        eprint("Error: Report subcommand can only operate on one policy")
        sys.exit(1)
    
    policy = policies.pop()
    odir = options.output_dir.rstrip(os.path.sep)
    if os.path.sep in odir and os.path.basename(odir) == policy.name:
        # policy sub-directory passed - ignore
        options.output_dir = os.path.split(odir)[0]
        # regenerate the execution context based on new path
        policy = Policy(policy.data, options)
    d = datetime.now()
    delta = timedelta(days=options.days)
    begin_date = d - delta
    do_report(
        policy, begin_date, options, sys.stdout,
        raw_output_fh=options.raw)


@policy_command
def logs(options, policies):
    if len(policies) == 0:
        eprint("Error: No policy supplied")
        sys.exit(1)

    if len(policies) > 1:
        eprint("Error: Log subcommand can only operate on one policy")
        sys.exit(1)

    policy = policies.pop()

    for e in policy.get_logs(options.start, options.end):
        print("%s: %s" % (
            time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(e['timestamp'] / 1000)),
            e['message']))


def _schema_get_docstring(starting_class):
    """ Given a class, return its docstring.

    If no docstring is present for the class, search base classes in MRO for a
    docstring.
    """
    for cls in inspect.getmro(starting_class):
        if inspect.getdoc(cls):
            return inspect.getdoc(cls)


def schema_cmd(options):
    """ Print info about the resources, actions and filters available. """
    if options.json:
        schema.json_dump(options.resource)
        return

    load_resources()
    resource_mapping = schema.resource_vocabulary()

    if options.summary:
        schema.summary(resource_mapping)
        return

    # Here are the formats for what we accept:
    # - No argument
    #   - List all available RESOURCES
    # - RESOURCE
    #   - List all available actions and filters for supplied RESOURCE
    # - RESOURCE.actions
    #   - List all available actions for supplied RESOURCE
    # - RESOURCE.actions.ACTION
    #   - Show class doc string and schema for supplied action
    # - RESOURCE.filters
    #   - List all available filters for supplied RESOURCE
    # - RESOURCE.filters.FILTER
    #   - Show class doc string and schema for supplied filter

    if not options.resource:
        resource_list = {'resources': sorted(resources.keys()) }
        print(yaml.safe_dump(resource_list, default_flow_style=False))
        return

    # Format is RESOURCE.CATEGORY.ITEM
    components = options.resource.split('.')

    #
    # Handle resource
    #
    resource = components[0].lower()
    if resource not in resource_mapping:
        eprint('Error: {} is not a valid resource'.format(resource))
        sys.exit(1)

    if len(components) == 1:
        del(resource_mapping[resource]['classes'])
        output = {resource: resource_mapping[resource]}
        print(yaml.safe_dump(output))
        return

    #
    # Handle category
    #
    category = components[1].lower()
    if category not in ('actions', 'filters'):
        eprint(("Error: Valid choices are 'actions' and 'filters'."
                " You supplied '{}'").format(category))
        sys.exit(1)

    if len(components) == 2:
        output = "No {} available for resource {}.".format(category, resource)
        if category in resource_mapping[resource]:
            output = {resource: {
                category: resource_mapping[resource][category]}}
        print(yaml.safe_dump(output))
        return

    #
    # Handle item
    #
    item = components[2].lower()
    if item not in resource_mapping[resource][category]:
        eprint('Error: {} is not in the {} list for resource {}'.format(
                item, category, resource))
        sys.exit(1)

    if len(components) == 3:
        cls = resource_mapping[resource]['classes'][category][item]

        # Print docstring
        docstring = _schema_get_docstring(cls)
        print("\nHelp\n----\n")
        if docstring:
            print(docstring)
        else:
            # Shouldn't ever hit this, so exclude from cover
            print("No help is available for this item.")  # pragma: no cover

        # Print schema
        print("\nSchema\n------\n")
        pp = pprint.PrettyPrinter(indent=4)
        if hasattr(cls, 'schema'):
            pp.pprint(cls.schema)
        else:
            # Shouldn't ever hit this, so exclude from cover
            print("No schema is available for this item.", file=sys.sterr)  # pragma: no cover
        print('')
        return

    # We received too much (e.g. s3.actions.foo.bar)
    eprint("Invalid selector '{}'.  Max of 3 components in the "
           "format RESOURCE.CATEGORY.ITEM".format(options.resource))
    sys.exit(1)


def _metrics_get_endpoints(options):
    """ Determine the start and end dates based on user-supplied options. """
    if bool(options.start) ^ bool(options.end):
        eprint('Error: --start and --end must be specified together')
        sys.exit(1)

    if options.start and options.end:
        start = options.start
        end = options.end
    else:
        end = datetime.utcnow()
        start = end - timedelta(options.days)

    return start, end


@policy_command
def metrics_cmd(options, policies):
    start, end = _metrics_get_endpoints(options)
    data = {}
    for p in policies:
        log.info('Getting %s metrics', p)
        data[p.name] = p.get_metrics(start, end, options.period)
    print(dumps(data, indent=2))


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
