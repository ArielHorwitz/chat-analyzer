from pathlib import Path
import random
import arrow
import pandas as pd
from matplotlib import pyplot as plt
import util


class Importer:
    CACHED_DF_NAME = 'cached_df.json'

    def __init__(self,
            import_file=None,
            output_folder=None,
            mode='randomgen',
            line_limit=None,
        ):
        self.output_folder = self.resolve_output(output_folder)

        import_methods = {
            'whatsapp': self.import_chat_whatsapp,
        }

        if mode == 'randomgen':
            print(f'Generating random data...')
            self.df = self.import_chat_random(line_limit=line_limit)
        elif mode == 'cached':
            print(f'Using cached data...')
            self.df = self.import_cached_dataframe(self.output_folder)
        elif mode in import_methods:
            print(f'Importing data from file...')
            self.df = import_methods[mode](Path(import_file), line_limit=line_limit)
            self.cache_dataframe(self.df)
        else:
            raise ValueError(f'No such mode: {mode}')

    @staticmethod
    def resolve_output(dir_path):
        if dir_path is None:
            dir_path = Path.cwd() / 'output'
        else:
            dir_path = Path(dir_path)
        if not dir_path.is_dir():
            dir_path.mkdir(parents=True)
        return dir_path

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


class Analyzer:
    def __init__(self, df, output_folder=None):
        self.df = df
        self.output_folder = self.resolve_output_folder(output_folder)

    @staticmethod
    def resolve_output_folder(output_folder):
        if output_folder is None:
            output_folder = Path.cwd() / 'output'
        output_folder = Path(output_folder)
        if not output_folder.is_dir():
            output_folder.mkdir(parents=True)
        return output_folder

    def analyze(self, show_dir=True):
        print(f'Analyzing...')
        print(self.df)
        self.all_days_range = self._all_days_range()
        self.per_day()
        self.per_hour()
        self.per_sender()
        self.per_sender_media()
        self.unique_messages()
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

    def per_day(self):
        print('Analyzing messages by day...')
        per_day = self.df.groupby('day').count()['sender']
        per_weekday = self.df.groupby('weekday').count()['sender']
        # Fill missing days
        per_day = per_day.reindex(self.all_days_range)
        per_day.fillna(0, inplace=True)
        per_weekday = per_weekday.reindex(self.all_weekdays)
        per_weekday.fillna(0, inplace=True)
        # Plot weekdays
        the_one_plot = per_weekday.plot(
            title='Messages per weekday',
            kind='bar',
            fontsize=18,
            xlabel='Day',
            ylabel='Messages',
            figsize=(20,15),
        )
        plt.savefig(self.output_folder / 'per-weekday.png')
        the_one_plot.clear()
        # Plot dates
        the_one_plot = per_day.plot(
            title='Messages per day',
            kind='bar',
            fontsize=18,
            xlabel='Day',
            ylabel='Messages',
            figsize=(20,15),
        )
        plt.savefig(self.output_folder / 'per-day.png')
        the_one_plot.clear()

    def per_hour(self):
        print('Analyzing messages by hour...')
        day_count = len(self.all_days_range)
        per_hour_total = self.df.groupby('hour').count()['date']
        per_hour = per_hour_total / day_count
        # Fill missing hours
        per_hour = per_hour.reindex([f'{_:0>2}' for _ in range(24)])
        per_hour.fillna(0, inplace=True)
        # Plot
        the_one_plot = per_hour.plot(
            title='Average messages per hour',
            kind='bar',
            fontsize=18,
            xlabel='Hour',
            ylabel='Average messages',
            figsize=(20,15),
        )
        plt.savefig(self.output_folder / 'per-hour.png')
        the_one_plot.clear()

    def per_sender(self):
        print('Analyzing senders...')
        msg_per_sender = self.df.groupby('sender').count()['message']
        # Plot
        the_one_plot = msg_per_sender.plot(
            title='Messages by sender',
            kind='pie',
            fontsize=18,
            figsize=(20,15),
        )
        plt.savefig(self.output_folder / 'per-sender.png')
        the_one_plot.clear()

    def per_sender_media(self):
        print('Analyzing media messages...')
        medias = self.df[self.df['message'] == '<Media omitted>']
        media_per_sender = medias.groupby('sender').count()['message']
        # Plot
        the_one_plot = media_per_sender.plot(
            title='Media messages by sender',
            kind='pie',
            fontsize=18,
            figsize=(20,15),
        )
        plt.savefig(self.output_folder / 'per-sender-media.png')
        the_one_plot.clear()

    def unique_messages(self):
        message_counts = self.df.groupby('message').count()['sender']
        message_counts = message_counts.sort_values(ascending=False)
        message_counts = message_counts[message_counts > 1]
        print(f'Analyzing unique messages...')
        mc_strs = []
        for idx, count in message_counts.iteritems():
            mc_strs.append(f'{count} - {idx}')
        util.file_dump(self.output_folder / 'unique_message_counts.txt', '\n'.join(mc_strs))


def main():
    arg_space = util.parse_args()
    df = Importer(arg_space.file, mode=arg_space.mode, line_limit=arg_space.line_limit).df
    Analyzer(df, arg_space.output).analyze(show_dir=arg_space.show_output)


if __name__ == '__main__':
    main()
