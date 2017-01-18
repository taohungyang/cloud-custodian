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

import argparse
import importlib
import logging
import pdb
import sys
import traceback
from datetime import datetime
from dateutil.parser import parse as date_parse

DEFAULT_REGION = 'us-east-1'


def _default_options(p, blacklist=""):
    """ Add basic options ot the subparser.

    `blacklist` is a list of options to exclude from the default set.
    e.g.: ['region', 'log-group']
    """
    provider = p.add_argument_group(
        "provider", "AWS account information, defaults per the aws cli")

    if 'region' not in blacklist:
        provider.add_argument(
            "-r", "--region", default=None,
            help="AWS Region to target (Default: %(default)s)")
    provider.add_argument(
        "--profile",
        help="AWS Account Config File Profile to utilize")
    provider.add_argument("--assume", default=None, dest="assume_role",
                          help="Role to assume")

    config = p.add_argument_group(
        "config", "Policy config file and policy selector")
    config.add_argument("-c", "--config", required=True,
                        help="Policy Configuration File")
    config.add_argument("-p", "--policies", default=None, dest='policy_filter',
                        help="Only use named/matched policies")
    config.add_argument("-t", "--resource", default=None, dest='resource_type',
                        help="Only use policies with the given resource type")

    p.add_argument("-v", "--verbose", action="store_true",
                   help="Verbose logging")
    p.add_argument("--debug", default=False, help=argparse.SUPPRESS,
                   action="store_true")

    if 'log-group' not in blacklist:
        p.add_argument(
            "-l", "--log-group", default=None,
            help="Cloudwatch Log Group to send policy logs")
    else:
        p.add_argument("--log-group", default=None, help=argparse.SUPPRESS)

    if 'output-dir' not in blacklist:
        p.add_argument("-s", "--output-dir", required=True,
                       help="Directory or S3 URL For policy output")

    if 'cache' not in blacklist:
        p.add_argument(
            "-f", "--cache", default="~/.cache/cloud-custodian.cache",
            help="Cache file (default %(default)s)")
        p.add_argument(
            "--cache-period", default=15, type=int,
            help="Cache validity in minutes (default %(default)i)")
    else:
        p.add_argument("--cache", default=None, help=argparse.SUPPRESS)


def _report_options(p):
    """ Add options specific to the report subcommand. """
    _default_options(p, blacklist=['region', 'cache', 'log-group'])
    p.add_argument(
        '--days', type=float, default=1,
        help="Number of days of history to consider")
    p.add_argument(
        '--raw', type=argparse.FileType('wb'),
        help="Store raw json of collected records to given file path")
    p.add_argument(
        '--field', action='append', default=[], type=_key_val_pair,
        metavar='HEADER=FIELD',
        help='Repeatable. JMESPath of field to include in the output OR '\
            'for a tag use prefix `tag:`')
    p.add_argument(
        '--no-default-fields', action="store_true",
        help='Exclude default fields for report.')

    # We don't include `region` because the report command ignores it
    p.add_argument("--region", default=DEFAULT_REGION, help=argparse.SUPPRESS)


def _metrics_options(p):
    """ Add options specific to metrics subcommand. """
    _default_options(p, blacklist=['log-group', 'output-dir', 'cache'])

    p.add_argument(
        '--start', type=date_parse,
        help='Start date (requires --end, overrides --days)')
    p.add_argument(
        '--end', type=date_parse, help='End date')
    p.add_argument(
        '--days', type=int, default=14,
        help='Number of days of history to consider (default: %(default)i)')
    p.add_argument('--period', type=int, default=60*24*24)


def _logs_options(p):
    """ Add options specific to logs subcommand. """
    _default_options(p, blacklist=['cache'])

    # default time range is 0 to "now" (to include all log entries)
    p.add_argument(
        '--start',
        default='the beginning',  # invalid, will result in 0
        help='Start date and/or time',
    )
    p.add_argument(
        '--end',
        default=datetime.now().strftime('%c'),
        help='End date and/or time',
    )


def _schema_options(p):
    """ Add options specific to schema subcommand. """

    p.add_argument('resource', metavar='selector', nargs='?', default=None)
    p.add_argument(
        '--summary', action="store_true",
        help="Summarize counts of available resources, actions and filters")
    p.add_argument('--json', action="store_true", help=argparse.SUPPRESS)
    p.add_argument(
        '-v', '--verbose', action="store_true",
        help="Verbose logging")
    p.add_argument("--debug", default=False, help=argparse.SUPPRESS)


def _dryrun_option(p):
    p.add_argument(
        "-d", "--dryrun", action="store_true",
        help="Don't execute actions but filter resources")


def _key_val_pair(value):
    """
    Type checker to ensure that --field values are of the format key=val
    """
    if '=' not in value:
        msg = 'values must be of the form `header=field`'
        raise argparse.ArgumentTypeError(msg)
    return value


def setup_parser():
    c7n_desc = "Cloud fleet management"
    parser = argparse.ArgumentParser(description=c7n_desc)

    # Setting `dest` means we capture which subparser was used.  We'll use it
    # later on when doing post-parsing validation.
    subs = parser.add_subparsers(dest='subparser')

    report_desc = "CSV report of resources that a policy matched/ran on"
    report = subs.add_parser(
        "report", description=report_desc, help=report_desc)
    report.set_defaults(command="c7n.commands.report")
    _report_options(report)

    logs_desc = "Get policy execution logs from s3 or cloud watch logs"
    logs = subs.add_parser(
        'logs', help=logs_desc, description=logs_desc)
    logs.set_defaults(command="c7n.commands.logs")
    _logs_options(logs)

    metrics_desc = "Retrieve metrics for policies from CloudWatch Metrics"
    metrics = subs.add_parser(
        'metrics', description=metrics_desc, help=metrics_desc)
    metrics.set_defaults(command="c7n.commands.metrics_cmd")
    _metrics_options(metrics)

    version = subs.add_parser(
        'version', help="Display installed version of custodian")
    version.set_defaults(command=cmd_version)
    version.add_argument(
        "-v", "--verbose", action="store_true",
        help="Verbose Logging")

    validate_desc = (
        "Validate config files against the custodian jsonschema")
    validate = subs.add_parser(
        'validate', description=validate_desc, help=validate_desc)
    validate.set_defaults(command="c7n.commands.validate")
    validate.add_argument(
        "-c", "--config",
        help="Policy Configuration File (old; use configs instead)")
    validate.add_argument("configs", nargs='*',
                          help="Policy Configuration File(s)")
    validate.add_argument("-v", "--verbose", action="store_true",
                          help="Verbose Logging")
    validate.add_argument("--debug", default=False, help=argparse.SUPPRESS)

    schema_desc = ("Browse the available vocabularies (resources, filters, and "
                   "actions) for policy construction. The selector "
                   "is specified with RESOURCE[.CATEGORY[.ITEM]] "
                   "examples: s3, ebs.actions, or ec2.filters.instance-age")
    schema = subs.add_parser(
        'schema', description=schema_desc,
        help="Interactive cli docs for policy authors")
    schema.set_defaults(command="c7n.commands.schema_cmd")
    _schema_options(schema)

    #access_desc = ("Show permissions needed to execute the policies")
    #access = subs.add_parser(
    #    'access', description=access_desc, help=access_desc)
    #access.set_defaults(command='c7n.commands.access')
    #_default_options(access)
    #access.add_argument(
    #    '-m', '--access', default=False, action='store_true')

    run_desc = ("Execute the policies in a config file")
    run = subs.add_parser("run", description=run_desc, help=run_desc)
    run.set_defaults(command="c7n.commands.run")
    _default_options(run)
    _dryrun_option(run)
    run.add_argument(
        "-m", "--metrics-enabled",
        default=False, action="store_true",
        help="Emit metrics to CloudWatch Metrics")

    return parser


def cmd_version(options):
    from c7n.version import version
    print(version)


def main():
    parser = setup_parser()
    options = parser.parse_args()

    level = options.verbose and logging.DEBUG or logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s: %(name)s:%(levelname)s %(message)s")
    logging.getLogger('botocore').setLevel(logging.ERROR)
    logging.getLogger('s3transfer').setLevel(logging.ERROR)

    try:
        command = options.command
        if not callable(command):
            command = getattr(
                importlib.import_module(command.rsplit('.', 1)[0]),
                command.rsplit('.', 1)[-1])
        command(options)
    except Exception:
        if not options.debug:
            raise
        traceback.print_exc()
        pdb.post_mortem(sys.exc_info()[-1])

