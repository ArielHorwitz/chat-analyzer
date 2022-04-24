from argparse import ArgumentParser
from pathlib import Path

def parse_args():
    parser = ArgumentParser(description='Analyze chat logs.')
    parser.add_argument(
        '-f', dest='file',
        type=Path, default=None,
        help='File to import and analyze')
    parser.add_argument(
        '-o', dest='output',
        type=Path, default=None,
        help='Directory to save analyzed data')
    parser.add_argument(
        '-l', dest='line_limit',
        type=int, default=10**9,
        help='Maximum number of lines to import from file')

    args = parser.parse_args()
    print(f'Parsed args: {args}')
    return args


def file_dump(file, d, clear=True):
    with open(file, 'w' if clear else 'a') as f:
        f.write(d)


def file_load(file):
    with open(file, 'r') as f:
        d = f.read()
    return d
