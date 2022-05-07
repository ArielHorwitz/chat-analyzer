import random
import arrow
from argparse import ArgumentParser
from pathlib import Path


LOREM_IPSUM = """cursus nibh velit venenatis est habitasse lectus odio aliquet metus varius nec habitant quis sodales dictumst euismod imperdiet ultrices mollis senectus himenaeos cursus hendrerit non quisque platea egestas viverra nam diam eget inceptos duis magna vulputate suspendisse neque pulvinar tempus sagittis aliquet libero blandit vivamus est pellentesque egestas laoreet auctor porta arcu consequat nullam"""
LOREM_IPSUM_WORDS = list(LOREM_IPSUM.split(' '))


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
        '-m', dest='mode',
        type=str, default='randomgen',
        help='Type of import, one of: randomgen, cached, whatsapp')
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


def generate_random_line(word_count=None):
    word_count = random.randint(1, 12) if word_count is None else word_count
    words = [random.choice(LOREM_IPSUM_WORDS) for i in range(word_count)]
    line = ' '.join(words).lower().capitalize()
    return line


def generate_random_date(week_range=3, day_range=7, hour_range=24):
    t = arrow.now()
    t = t.shift(weeks=random.randint(-week_range-1, 0))
    t = t.shift(days=random.randint(-day_range-1, 0))
    t = t.shift(hours=random.randint(-hour_range-1, 0))
    return t
