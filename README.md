# Chat analyzer
A simple chat log analyzer.

### Features
- Import whatsapp chats (as exported to text)
- Generate random chats
- Sender name anonymization (please note that anonymity is very weak in analysis data)

### Data produced
- Messages per day
- Messages per hour
- Messages per person
- Most common unique messages
- Wordclouds

### Installation
Assuming you have [pyenv](https://github.com/pyenv/pyenv) and [poetry](https://python-poetry.org/) installed:
```
poetry install
```

The analysis requires language data:
```
poetry run python -m spacy download en_core_web_sm
```

### Basic usage
For new or updated chats:
```
poetry run python analyzer/main.py -m whatsapp -f path/to/chat.txt -o output/dir/ --cache -a all
```
For reanalyzing the same chat:
```
poetry run python analyzer/main.py -m cached -o output/dir/ -a all
```
See more options:
```
poetry run python analyzer/main.py --help
```
