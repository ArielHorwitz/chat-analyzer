from pathlib import Path
import copy
import random
import arrow
from collections import Counter
import pandas as pd
from plotly import express as px
from wordcloud import WordCloud
import spacy
import util


try:
    print(f'Loading language data...')
    NLP = spacy.load('en_core_web_sm')
except OSError:
    raise RuntimeError(f'Missing data to download. Please run the following command from within your venv: "python -m spacy download en_core_web_sm"')

DATE_FORMAT = 'YYYY-MM-DD HH:mm:ss'
MEDIA_MESSAGE = '<Media omitted>'


class Importer:
    CACHED_DF_NAME = 'cached_df.json'

    def __init__(self,
            import_file=None, output_folder=None,
            mode='randomgen', line_limit=None, anonymize_senders=True,
            cache_data=False,
        ):
        self.output_folder = output_folder
        self.__salt = str(random.random())[-4:]
        self.__crypto_names = copy.copy(util.CRYPTO_NAMES)
        random.shuffle(self.__crypto_names)
        self.__sender_map = {}
        # Get data
        import_methods = {'whatsapp': self.import_chat_whatsapp}
        if mode == 'randomgen':
            print(f'Generating random data...')
            self.df = self.import_chat_random(line_limit=line_limit)
        elif mode == 'cached':
            print(f'Using cached data...')
            self.df = self.import_cached_dataframe(self.output_folder)
        elif mode in import_methods:
            print(f'Importing data from file...')
            self.df = import_methods[mode](Path(import_file), line_limit=line_limit)
        else:
            raise ValueError(f'No such mode: {mode}')
        # Process data
        if anonymize_senders:
            self.df['sender'] = [self._anonymize_sender(_) for _ in self.df['sender']]
        # Post-import
        print(f'Import completed.')
        with pd.option_context('display.min_rows', 30):
            print(self.df)
        if cache_data:
            self.cache_dataframe(self.df)

    def _anonymize_sender(self, name):
        if name in self.__sender_map:
            return self.__sender_map[name]
        if self.__crypto_names:
            self.__sender_map[name] = self.__crypto_names.pop(0)
        else:
            self.__sender_map[name] = util.h256(f'{name}{self.__salt}')[:6]
        return self.__sender_map[name]

    @classmethod
    def import_cached_dataframe(cls, cache_dir):
        cache_dir = Path(cache_dir)
        cached_df_json = cache_dir / cls.CACHED_DF_NAME
        if not cached_df_json.is_file():
            raise FileNotFoundError(f'Missing cached chat file from {cached_df_json}')
        return pd.read_json(cached_df_json)

    def cache_dataframe(self, df):
        output = self.output_folder / self.CACHED_DF_NAME
        df.to_json(output)
        print(f'Cached chat at: {output}')

    @staticmethod
    def import_chat_whatsapp(target_file, line_limit=0):
        line_limit = 10**9 if line_limit == 0 else line_limit
        print(f'Processing whatsapp chat: {target_file} (max: {line_limit} lines)')
        chat_content = util.file_load(target_file)
        chat_lines = chat_content.split('\n')
        chat_df = pd.DataFrame(columns=['date', 'sender', 'message'])
        print(f'Number of lines in chat: {len(chat_lines)}')
        # Process messages
        for i, line in enumerate(chat_lines[:line_limit]):
            if i % 1000 == 0:
                print(f'Processing line #{i}')
            try:
                date, line = line.split(' - ', 1)
                sender, message = line.split(': ', 1)
                date = arrow.get(date, 'M/D/YY, HH:mm').format(DATE_FORMAT)
                chat_df.loc[len(chat_df.index)] = [date, sender, message]
            except Exception:
                if len(chat_df['date']) > 0:
                    chat_df.loc[len(chat_df.index)-1, 'message'] += f'\n{line}'
                else:
                    print(f'Failed to process line: {line}')

        print(f'Post processing days...')
        chat_df['day'] = chat_df['date'].apply(lambda x: arrow.get(x).format('YYYY-MM-DD'))
        chat_df['weekday'] = chat_df['date'].apply(lambda x: arrow.get(x).format('dddd'))
        print(f'Post processing hours...')
        chat_df['hour'] = chat_df['date'].apply(lambda x: arrow.get(x).format('HH'))

        print(f'Imported whatsapp chat.')
        return chat_df

    @staticmethod
    def import_chat_random(line_limit=0, senders=None):
        line_limit = 2_000 if line_limit == 0 else line_limit
        print(f'Creating random chat history with {line_limit} messages')
        chat_df = pd.DataFrame(columns=['date', 'sender', 'message', 'day', 'weekday', 'hour'])
        if senders is None:
            senders = ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve']
        # Generate messages
        for i in range(line_limit):
            message = util.generate_random_line()
            sender = random.choice(senders)
            msg_date = util.generate_random_date()
            date = msg_date.format(DATE_FORMAT)
            day = msg_date.format('YYYY-MM-DD')
            weekday = msg_date.format('dddd')
            hour = msg_date.format('HH')
            if i % 1000 == 0:
                print(f'Generating message #{i}: ({sender} @ {date}) {message}')
            chat_df.loc[len(chat_df.index)] = [date, sender, message, day, weekday, hour]

        print(f'Generated random chat.')
        return chat_df


class Analyzer:
    def __init__(self, df, output_folder, font_path=None):
        self.df = df
        self.output_folder = output_folder
        self.font_path = None if font_path is None else Path(font_path)
        self.all_days_range = self._all_days_range()

    @classmethod
    def _get_analyses_map(cls):
        return {
            'time': [cls.per_day, cls.per_hour],
            'counts': [cls.per_sender, cls.per_sender_media, cls.unique_messages],
            'cloud': [cls.full_wordcloud, cls.per_sender_wordclouds],
        }

    def analyze(self, show_dir=True, analyses=None):
        analyses_map = self._get_analyses_map()
        if not analyses:  # is None or empty list
            analyses = ['all']
        if 'all' in analyses:
            analyses = list(analyses_map.keys())
        print(f'Analyzing: {", ".join(analyses)}')
        for analysis_category in analyses:
            for analysis in analyses_map[analysis_category]:
                analysis(self)
        print(f'Output data to: {self.output_folder}')
        if show_dir:
            util.open_file_explorer(self.output_folder)

    def _all_days_range(self):
        raw_days = sorted(self.df.groupby('day').count().index)
        first_day = arrow.get(raw_days[0])
        last_day = arrow.get(raw_days[-1])
        day_range = arrow.Arrow.range('day', first_day, last_day)
        all_days = [_.format('YYYY-MM-DD') for _ in day_range]
        return all_days

    all_weekdays = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    all_hours = [f'{_:0>2}' for _ in range(24)]

    def per_day(self):
        print('Analyzing messages by day...')
        # Dates
        per_day = self.df.groupby(['day', 'sender']).size().unstack(level=0)
        per_day = per_day.reindex(columns=self.all_days_range)
        per_day.fillna(0, inplace=True)
        per_day = per_day.T
        # Weekdays
        per_weekday = self.df.groupby(['weekday', 'sender']).size().unstack(level=0)
        per_weekday = per_weekday.reindex(columns=self.all_weekdays)
        per_weekday.fillna(0, inplace=True)
        per_weekday = per_weekday.T
        day_labels = {'day': 'Date', 'sender': 'Sender', 'value': 'Messages'}
        weekday_labels = {'weekday': 'Day', 'sender': 'Sender', 'value': 'Messages'}
        # Figures
        fig = px.bar(per_day, title='Messages per day (stacked)', labels=day_labels)
        fig.write_html(self.output_folder / f'msg-per-day-stacked.html')
        fig = px.bar(per_day, barmode='group', title='Messages per day', labels=day_labels)
        fig.write_html(self.output_folder / f'msg-per-day.html')
        fig = px.bar(per_weekday, title='Messages per weekday (stacked)', labels=weekday_labels)
        fig.write_html(self.output_folder / f'msg-per-weekday-stacked.html')
        fig = px.bar(per_weekday, barmode='group', title='Messages per weekday', labels=weekday_labels)
        fig.write_html(self.output_folder / f'msg-per-weekday.html')

    def per_hour(self):
        print('Analyzing messages by hour...')
        per_hour = self.df.groupby(['hour', 'sender']).size().unstack(level=0)
        per_hour.rename(columns=lambda x: f'{x:0>2}', inplace=True)
        per_hour = per_hour.reindex(columns=self.all_hours)
        per_hour.fillna(0, inplace=True)
        per_hour = per_hour.T
        per_hour /= len(self.all_days_range)
        labels = {'hour': 'Hour', 'sender': 'Sender', 'value': 'Messages'}
        # Figures
        fig = px.bar(per_hour, title='Messages per hour (stacked)', labels=labels)
        fig.write_html(self.output_folder / f'msg-per-hour-stacked.html')
        fig = px.bar(per_hour, barmode='group', title='Messages per hour', labels=labels)
        fig.write_html(self.output_folder / f'msg-per-hour.html')

    def per_sender(self):
        print('Analyzing senders...')
        msg_per_sender = self.df.groupby('sender').size().to_frame(name='Messages')
        labels = {'sender': 'Sender'}
        fig = px.pie(
            msg_per_sender, title='Messages per person',
            names=msg_per_sender.index, values='Messages', labels=labels)
        fig.write_html(self.output_folder / f'msg-per-sender.html')

    def per_sender_media(self):
        print('Analyzing media messages...')
        medias = self.df[self.df['message'] == MEDIA_MESSAGE]
        media_per_sender = medias.groupby('sender').size().to_frame(name='Messages')
        fig = px.pie(media_per_sender, names=media_per_sender.index, values='Messages', title='Media messages per person')
        fig.write_html(self.output_folder / f'msg-media-per-sender.html')

    def unique_messages(self):
        print(f'Analyzing unique messages...')
        message_counts = self.df.groupby('message').count()['sender']
        message_counts = message_counts.sort_values(ascending=False)
        message_counts = message_counts[message_counts > 1]
        mc_strs = []
        for idx, count in message_counts.iteritems():
            mc_strs.append(f'{count} - {idx}')
        util.file_dump(self.output_folder / 'unique_message_counts.txt', '\n'.join(mc_strs))

    def full_wordcloud(self):
        all_text = ' '.join(self.df['message'])
        print(f'Generating wordcloud ({len(all_text):,} chars)...')
        self.generate_wordcloud(all_text, name='all')

    def per_sender_wordclouds(self):
        max_senders = 5
        print(f'Analyzing message contents of top {max_senders} senders...')
        msg_per_sender = self.df.groupby('sender').size().sort_values(ascending=False)
        print(msg_per_sender)
        for sender_name in msg_per_sender.index[:max_senders]:
            all_msgs = self.df[self.df['sender'] == sender_name]['message']
            all_text = ' '.join(all_msgs)
            print(f'Generating wordcloud for {sender_name} ({len(all_text):,} chars)...')
            self.generate_wordcloud(all_text, name=sender_name.lower())

    def generate_wordcloud(self, text, name):
        # Tokenize
        doc = NLP(text)
        words = [token.text.lower() for token in doc]
        unique_words = set(words)
        word_counts = Counter(words)
        # Wordcloud
        wc_kwargs = {
            'width': 1200,
            'height': 800,
            'max_words': 500,
        }
        if self.font_path is not None and self.font_path.is_file():
            wc_kwargs |= {'font_path': str(self.font_path)}
        wc = WordCloud(**wc_kwargs).generate_from_frequencies(word_counts)
        wc.to_file(self.output_folder / f'wordcloud-{name}.png')


def main():
    arg_space = util.parse_args()
    output_dir = util.resolve_output(arg_space.output,
        clear=arg_space.clear, force_clear=arg_space.force_clear,
        ignore=[Importer.CACHED_DF_NAME],
        )
    df = Importer(
        import_file=arg_space.file, output_folder=output_dir,
        mode=arg_space.mode, line_limit=arg_space.line_limit,
        anonymize_senders=arg_space.anonymize, cache_data=arg_space.cache_data,
        ).df
    a = Analyzer(df, output_folder=output_dir, font_path=arg_space.font_path)
    a.analyze(show_dir=arg_space.show_output, analyses=arg_space.analyses)


if __name__ == '__main__':
    main()
