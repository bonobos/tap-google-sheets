#!/usr/bin/env python3

import argparse
import json
import sys

import singer
from singer import Catalog

from tap_google_sheets.client import GoogleClient
from tap_google_sheets.discover import discover
from tap_google_sheets.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'client_id',
    'client_secret',
    'refresh_token',
    'spreadsheet_id',
    'start_date',
    'user_agent'
]


def do_discover(select_all, client, spreadsheet_id):
    LOGGER.info('Starting discover')
    catalog = discover(select_all, client, spreadsheet_id)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


def parse_args(required_config_keys):
    '''Parse standard command-line args.

    -c,--config     Config file
    -s,--state      State file
    -d,--discover   Run in discover mode
    -a,--select_all Select all streams and fields for discover mode
    --catalog       Catalog file

    Returns the parsed args object from argparse. For each argument that
    point to JSON files (config, state, properties), we will automatically
    load and parse the JSON file.
    '''
    parser = argparse.ArgumentParser()

    parser.add_argument('--config', '-c', help='Config file', required=True)
    parser.add_argument('--state', '-s', help='State file')
    parser.add_argument('--catalog', help='Catalog file')
    parser.add_argument('--discover', '-d', action='store_true', help='Do schema discovery')
    parser.add_argument('--select_all', '-a', action='store_true',
                        help='Select all streams and fields in discover mode')

    args = parser.parse_args()
    if args.config:
        setattr(args, 'config_path', args.config)
        args.config = load_json(args.config)
    if args.state:
        setattr(args, 'state_path', args.state)
        args.state = load_json(args.state)
    else:
        args.state = {}
    if args.catalog:
        setattr(args, 'catalog_path', args.catalog)
        args.catalog = Catalog.load(args.catalog)
    if args.select_all and not args.discover:
        parser.error('Select all only available for discovery mode')

    check_config(args.config, required_config_keys)
    return args


def check_config(config, required_keys):
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise Exception("Config is missing required keys: {}".format(missing_keys))


def load_json(path):
    with open(path) as file:
        return json.load(file)


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = parse_args(REQUIRED_CONFIG_KEYS)

    with GoogleClient(parsed_args.config['client_id'], parsed_args.config['client_secret'],
                      parsed_args.config['refresh_token'],
                      parsed_args.config['user_agent']) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        config = parsed_args.config
        spreadsheet_id = config.get('spreadsheet_id')

        if parsed_args.discover:
            do_discover(parsed_args.select_all, client, spreadsheet_id)
        elif parsed_args.catalog:
            sync(client=client, config=config, catalog=parsed_args.catalog, state=state)


if __name__ == '__main__':
    main()
