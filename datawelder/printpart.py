import argparse
import json
import os
import pickle

import datawelder.readwrite


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('--format', choices=('csv', 'json', 'pickle'), default='pickle')
    args = parser.parse_args()

    if args.format != 'pickle':
        config_path = os.path.join(os.path.dirname(args.path), 'datawelder.json')
        with datawelder.readwrite.open(config_path) as fin:
            config = json.load(fin)

    with datawelder.readwrite.open(args.path, 'rb') as fin:
        while True:
            try:
                record = pickle.load(fin)
            except EOFError:
                break

            if args.format == 'json':
                record_json = dict(zip(config['field_names'], record))
                print(json.dumps(record_json))
            else:
                print(record)


if __name__ == '__main__':
    main()
