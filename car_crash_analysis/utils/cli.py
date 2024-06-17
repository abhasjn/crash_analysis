"command line module"

from __future__ import print_function

import argparse
import logging
import os

logger = logging.getLogger(__name__)
cwd = os.getcwd()


def main():
    """main function for command line usage"""
    parser = argparse.ArgumentParser(description="Car Crash Analysis")
    parser.add_argument('--verbosity', '-v', type=int, dest='verbosity', help='verbosity range [0-6]', default=2,
                        required=False)

    command_parser = parser.add_subparsers(help="options", dest='command')

    infra_parser = command_parser.add_parser('run', help='Runs the analysis')
    infra_parser.add_argument('--number', '-n', type=str, dest='number',
                              help='Number of analysis to run, e.g: Give 1 to run only analysis 1 or 2,3,4 to run these 3 analysis or All to run all analysis',
                              required=True)

    args = parser.parse_args()

    logger.debug('Running with args: %s', args)

    if args.verbosity == 0:
        logging.basicConfig(level=logging.CRITICAL)
    elif args.verbosity == 1:
        logging.basicConfig(level=logging.WARNING)
    elif args.verbosity == 2:
        logging.basicConfig(level=logging.INFO)
    elif args.verbosity >= 3:
        logging.basicConfig(level=logging.DEBUG)

    if args.command == 'run':
        run_analysis(args)
    else:
        parser.print_help()


def run_analysis(args):
    from main import run_analysis

    analysis_num = args.number
    if analysis_num.lower() == "all":
        run_analysis("all")
    else:
        run_analysis(analysis_num.strip().split(","))
