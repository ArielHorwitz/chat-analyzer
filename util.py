import os, platform, subprocess
import hashlib
import random
import arrow
from argparse import ArgumentParser
from pathlib import Path


CRYPTO_NAMES = ['Alice', 'Bob', 'Carol', 'Dave', 'Eve', 'Frank', 'Grace', 'Ivan', 'Judy', 'Mike', 'Niaj', 'Olivia', 'Peggy', 'Rupert', 'Sybil', 'Ted', 'Victor', 'Wendy']
LOREM_IPSUM = """cursus nibh velit venenatis est habitasse lectus odio aliquet metus varius nec habitant quis sodales dictumst euismod imperdiet ultrices mollis senectus himenaeos cursus hendrerit non quisque platea egestas viverra nam diam eget inceptos duis magna vulputate suspendisse neque pulvinar tempus sagittis aliquet libero blandit vivamus est pellentesque egestas laoreet auctor porta arcu consequat nullam"""
LOREM_IPSUM_WORDS = list(LOREM_IPSUM.split(' '))


def parse_args():
    parser = ArgumentParser(description='Analyze chat logs.')
    parser.add_argument(
        '-m', dest='mode',
        type=str, default='randomgen',
        help='Type of import, one of: randomgen, cached, whatsapp')
    parser.add_argument(
        '-f', dest='file',
        type=Path, default=None,
        help='File to import and analyze')
    parser.add_argument(
        '--line-limit', dest='line_limit',
        type=int, default=0,
        help='Maximum number of lines to import from file')
    parser.add_argument(
        '--no-anon', dest='anonymize', action='store_false',
        help='Disable sender anonymization')
    parser.add_argument(
        '--cache', dest='cache_data', action='store_true',
        help='Cache imported data')
    parser.add_argument(
        '-a', dest='analyses',
        type=str, default='', nargs='*',
        help='Analyses, any combination of: all, time, counts, cloud')
    parser.add_argument(
        '-l', dest='lang_code',
        type=str, default='en',
        help='Analysis language code, e.g. en, nl, zh, etc. See: https://spacy.io/usage/models/#languages')
    parser.add_argument(
        '-o', dest='output',
        type=Path, default=None,
        help='Directory to save analyzed data')
    parser.add_argument(
        '--clear', dest='clear', action='store_true',
        help='Clear the output directory before analysis')
    parser.add_argument(
        '--force-clear', dest='force_clear', action='store_true',
        help='Like --clear but supresses prompts')
    parser.add_argument(
        '--font', dest='font_path',
        type=Path, default=None,
        help='Path to font to use for output')
    parser.add_argument(
        '-s', dest='show_output', action='store_true',
        help='Open the output folder')

    args = parser.parse_args()
    print(f'Parsed args: {args}')
    return args


def resolve_output(output_path, clear=False, force_clear=False, ignore=None):
    if output_path is None or output_path == '':
        output_path = Path.cwd() / 'output'
    if ignore is None:
        ignore = []
    output_path = Path(output_path)
    if not output_path.is_dir():
        output_path.mkdir(parents=True)
    if clear or force_clear:
        print(f'Clearing output folder: {output_path}')
        children = sorted(list(output_path.iterdir()), key=lambda x: str(x))
        for child in children:
            for ignored in ignore:
                if ignored in str(child):
                    break
            else:
                confirm = True
                if not force_clear:
                    confirm_input = input(f'Delete {child.name}? (y/n) ')
                    confirm = confirm_input.lower() == 'y'
                if not confirm:
                    continue
                print(f'Deleting {child.name} ...')
                child.unlink()
    return output_path


def file_dump(file, d, clear=True):
    with open(file, 'w' if clear else 'a') as f:
        f.write(d)


def file_load(file):
    with open(file, 'r') as f:
        d = f.read()
    return d


def open_file_explorer(path=None):
    if path is None:
        path = CWD
    if platform.system() == 'Windows':
        os.startfile(path)
    elif platform.system() == 'Darwin':
        subprocess.Popen(['open', path])
    else:
        subprocess.Popen(['xdg-open', path])


def generate_random_line(word_count=None):
    word_count = random.randint(1, 12) if word_count is None else word_count
    words = [random.choice(LOREM_IPSUM_WORDS) for i in range(word_count)]
    line = ' '.join(words).lower().capitalize()
    return line


def generate_random_date(week_range=3, day_range=7, hour_range=24, minute_range=60):
    t = arrow.now()
    t = t.shift(weeks=random.randint(-week_range, 0))
    t = t.shift(days=random.randint(-day_range, 0))
    t = t.shift(hours=random.randint(-hour_range, 0))
    t = t.shift(minutes=random.randint(-minute_range, 0))
    return t


def h256(input_str):
    return hashlib.sha256(input_str.encode()).hexdigest()
