import unittest
import os

from unittest.mock import patch
import datetime

import target_bigquery


class TestTargetBigQuery(unittest.TestCase):

    def setUp(self):
        self.config = {}

    @patch('target_bigquery.NamedTemporaryFile')
    @patch('target_bigquery.flush_streams')
    @patch('target_bigquery.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(self, dbSync_mock, flush_streams_mock, temp_file_mock):
        self.config['batch_size_rows'] = 20
        self.config['flush_all_streams'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_bigquery.persist_lines(self.config, lines)

        flush_streams_mock.assert_called_once()

    def test_adjust_timestamps_in_record(self):
        record = {
            'key1': '1',
            'key2': '2030-01-22',
            'key3': '10000-01-22 12:04:22',
            'key4': '25:01:01',
            'key5': 'I\'m good',
            'key6': None
        }

        schema = {
            'properties': {
                'key1': {
                    'type': ['null', 'string', 'integer'],
                },
                'key2': {
                    'anyOf': [
                        {'type': ['null', 'string'], 'format': 'date'},
                        {'type': ['null', 'string']}
                    ]
                },
                'key3': {
                    'type': ['null', 'string'], 'format': 'date-time',
                },
                'key4': {
                    'anyOf': [
                        {'type': ['null', 'string'], 'format': 'time'},
                        {'type': ['null', 'string']}
                    ]
                },
                'key5': {
                    'type': ['null', 'string'],
                },
                'key6': {
                    'type': ['null', 'string'], 'format': 'time',
                },
            }
        }

        target_bigquery.stream_utils.adjust_timestamps_in_record(record, schema)

        self.assertDictEqual({
            'key1': '1',
            'key2':  datetime.datetime(2030, 1, 22, 0, 0),
            'key3': datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
            'key4': datetime.timedelta(693595, 86399, 999999),
            'key5': 'I\'m good',
            'key6': None
        }, record)
