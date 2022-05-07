from pathlib import Path
import copy
import random
import arrow
import pandas as pd
from plotly import express as px
import util


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
                date = arrow.get(date, 'M/D/YY, HH:mm').format('YYYY-MM-DD HH:MM:SS')
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
        print(chat_df)
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
            date = util.generate_random_date()
            day = date.format('YYYY-MM-DD')
            weekday = date.format('dddd')
            hour = date.format('HH')
            if i % 1000 == 0:
                print(f'Generating message #{i}: ({sender} @ {date}) {message}')
            chat_df.loc[len(chat_df.index)] = [date, sender, message, day, weekday, hour]

        print(f'Generated random chat.')
        print(chat_df)
        return chat_df

MEDIA_MESSAGE = '<Media omitted>'

class Analyzer:
    def __init__(self, df, output_folder=None):
        self.df = df
        self.output_folder = output_folder

    def analyze(self, show_dir=True):
        print(f'Analyzing...')
        print(self.df.head(25))
        print(self.df.tail(25))
        self.all_days_range = self._all_days_range()
        figures = {
            **self.per_day(),
            **self.per_hour(),
            **self.per_sender(),
            **self.per_sender_media(),
            **self.unique_messages(),
        }
        for name, data in figures.items():
            data.write_html(self.output_folder / f'{name}.html')
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
        return {
            'msg-per-day-stacked': px.bar(per_day, title='Messages per day (stacked)', labels=day_labels),
            'msg-per-day': px.bar(per_day, barmode='group', title='Messages per day', labels=day_labels),
            'msg-per-weekday-stacked': px.bar(per_weekday, title='Messages per weekday (stacked)', labels=weekday_labels),
            'msg-per-weekday': px.bar(per_weekday, barmode='group', title='Messages per weekday', labels=weekday_labels),
        }

    def per_hour(self):
        print('Analyzing messages by hour...')
        per_hour = self.df.groupby(['hour', 'sender']).size().unstack(level=0)
        per_hour.rename(columns=lambda x: f'{x:0>2}', inplace=True)
        per_hour = per_hour.reindex(columns=self.all_hours)
        per_hour.fillna(0, inplace=True)
        per_hour = per_hour.T
        per_hour /= len(self.all_days_range)
        labels = {'hour': 'Hour', 'sender': 'Sender', 'value': 'Messages'}
        return {
            'msg-per-hour-stacked': px.bar(per_hour, title='Messages per hour (stacked)', labels=labels),
            'msg-per-hour': px.bar(per_hour, barmode='group', title='Messages per hour', labels=labels),
        }

    def per_sender(self):
        print('Analyzing senders...')
        msg_per_sender = self.df.groupby('sender').size().to_frame(name='Messages')
        labels = {'sender': 'Sender'}
        return {'msg-per-sender': px.pie(msg_per_sender, title='Messages per person', names=msg_per_sender.index, values='Messages', labels=labels)}

    def per_sender_media(self):
        print('Analyzing media messages...')
        medias = self.df[self.df['message'] == MEDIA_MESSAGE]
        media_per_sender = medias.groupby('sender').size().to_frame(name='Messages')
        return {'msg-media-per-sender': px.pie(media_per_sender, names=media_per_sender.index, values='Messages', title='Media messages per person')}

    def unique_messages(self):
        print(f'Analyzing unique messages...')
        message_counts = self.df.groupby('message').count()['sender']
        message_counts = message_counts.sort_values(ascending=False)
        message_counts = message_counts[message_counts > 1]
        mc_strs = []
        for idx, count in message_counts.iteritems():
            mc_strs.append(f'{count} - {idx}')
        util.file_dump(self.output_folder / 'unique_message_counts.txt', '\n'.join(mc_strs))
        return {}


def main():
    arg_space = util.parse_args()
    output_dir = util.resolve_output(arg_space.output)
    df = Importer(
        import_file=arg_space.file, output_folder=output_dir,
        mode=arg_space.mode, line_limit=arg_space.line_limit,
        anonymize_senders=arg_space.anonymize, cache_data=arg_space.cache_data,
        ).df
    Analyzer(df, output_folder=output_dir).analyze(show_dir=arg_space.show_output)


if __name__ == '__main__':
    main()
