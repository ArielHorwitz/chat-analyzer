# Chat analyzer
A simple chat log analyzer.

### Installation
Install requirements (consider using a [virtual environment](https://docs.python.org/3/tutorial/venv.html)):

`pip install -r requirements.txt`

The analysis requires language data:

`python -m spacy download en_core_web_sm`

To run:

`python main.py --help`

### Installation

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
