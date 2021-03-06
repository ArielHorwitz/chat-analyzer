from pathlib import Path
import copy
import random
import arrow
from collections import Counter, defaultdict
import pandas as pd
from plotly import express as px
from wordcloud import WordCloud
import spacy
import util
import plotly_html


DATE_FORMAT = 'YYYY-MM-DD HH:mm:ss'
WHATSPP_MEDIA_MESSAGE = '<Media omitted>'


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
        chat_df['is_media'] = chat_df['message'] == WHATSPP_MEDIA_MESSAGE

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
        chat_df['is_media'] = False

        print(f'Generated random chat.')
        return chat_df


class Analyzer:
    def __init__(self, df, output_folder, font_path=None,
            lang_code='en', strong_pos_filter=True, custom_word_filter=None,
        ):
        self.figures = defaultdict(list)
        self.df = df
        self.df_nomedia = self.df[~self.df.is_media]
        self.output_folder = output_folder
        self.font_path = None if font_path is None else Path(font_path)
        self.all_days_range = self._all_days_range()
        self.lang_code = lang_code
        self.nlp = self._get_nlp()
        self.custom_word_filter = set() if custom_word_filter is None else set(custom_word_filter)
        self.strong_pos_filter = strong_pos_filter

    def add_figure(self, fig, category):
        self.figures[category].append(fig)

    def _get_nlp(self):
        print(f'Loading {self.lang_code} language data...')
        try:
            return spacy.load(f'{self.lang_code}_core_web_sm')
        except OSError:
            print(f'Missing data to download. Please run the following command from within your venv: "python -m spacy download {self.lang_code}_core_web_sm"')
        return None

    @classmethod
    def _get_analyses_map(cls):
        return {
            'time': [cls.per_day, cls.per_weekday, cls.per_hour],
            'counts': [cls.per_sender, cls.per_sender_media, cls.common_messages],
            'cloud': [cls.full_wordcloud, cls.per_sender_wordclouds],
        }

    def analyze(self, analyses=None):
        analyses_map = self._get_analyses_map()
        if not analyses:  # is None or empty list
            analyses = ['all']
        if 'all' in analyses:
            analyses = list(analyses_map.keys())
        print(f'Analyzing: {", ".join(analyses)}')
        for analysis_category in analyses:
            for analysis in analyses_map[analysis_category]:
                analysis(self)

    def export_figures(self):
        plotly_html.write_css(self.output_folder)
        for category, figs in self.figures.items():
            file = self.output_folder / f'{category}.html'
            plotly_html.write_html(figs, file, title=category.capitalize())
        print(f'Output data to: {self.output_folder}')

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
        per_day_grouped = self.df.groupby(['day', 'sender']).size()
        per_day = per_day_grouped.index.to_frame()
        per_day['messages'] = per_day_grouped
        # Figures
        labels = {'day': 'Date', 'sender': 'Sender'}
        fig_args = dict(x='day', y='messages', nbins=100, color='sender', labels=labels)
        fig = px.histogram(per_day, title='Messages per day (stacked)', **fig_args)
        self.add_figure(fig, 'time')
        fig = px.histogram(per_day, title='Messages per day', barmode='group', **fig_args)
        self.add_figure(fig, 'time')

    def per_weekday(self):
        print('Analyzing messages by weekday...')
        per_weekday = self.df.groupby(['weekday', 'sender']).size().unstack(level=0)
        per_weekday = per_weekday.reindex(columns=self.all_weekdays)
        per_weekday.fillna(0, inplace=True)
        per_weekday = per_weekday.T
        # Figures
        labels = {'weekday': 'Day', 'sender': 'Sender', 'value': 'Messages'}
        fig = px.bar(per_weekday, title='Messages per weekday (stacked)', labels=labels)
        self.add_figure(fig, 'time')
        fig = px.bar(per_weekday, barmode='group', title='Messages per weekday', labels=labels)
        self.add_figure(fig, 'time')

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
        self.add_figure(fig, 'time')
        fig = px.bar(per_hour, barmode='group', title='Messages per hour', labels=labels)
        self.add_figure(fig, 'time')

    def per_sender(self):
        print('Analyzing senders...')
        msg_per_sender = self.df.groupby('sender').size().to_frame(name='Messages')
        labels = {'sender': 'Sender'}
        fig = px.pie(
            msg_per_sender, title='Total messages per person',
            names=msg_per_sender.index, values='Messages', labels=labels)
        self.add_figure(fig, 'counts')

    def per_sender_media(self):
        print('Analyzing media messages...')
        medias = self.df[self.df['is_media']]
        media_per_sender = medias.groupby('sender').size().to_frame(name='Messages')
        fig = px.pie(media_per_sender, names=media_per_sender.index, values='Messages', title='Media messages per person')
        self.add_figure(fig, 'counts')

    def common_messages(self):
        print(f'Analyzing common messages...')
        message_counts = self.df_nomedia.groupby('message').count()['sender']
        message_counts = message_counts.sort_values(ascending=False)
        message_counts = message_counts[message_counts > 1]
        mc_strs = []
        for idx, count in message_counts.iteritems():
            mc_strs.append(f'{count} - {idx}')
        util.file_dump(self.output_folder / 'common_messages.txt', '\n'.join(mc_strs))

    def full_wordcloud(self):
        all_text = ' '.join(self.df_nomedia['message'])
        print(f'Generating wordcloud ({len(all_text):,} chars)...')
        self.generate_wordcloud(all_text, name='all')

    def per_sender_wordclouds(self):
        max_senders = 5
        print(f'Analyzing message contents of top {max_senders} senders...')
        msg_per_sender = self.df_nomedia.groupby('sender').size().sort_values(ascending=False)
        print(msg_per_sender)
        for sender_name in msg_per_sender.index[:max_senders]:
            all_msgs = self.df_nomedia[self.df_nomedia['sender'] == sender_name]['message']
            all_text = ' '.join(all_msgs)
            print(f'Generating wordcloud for {sender_name} ({len(all_text):,} chars)...')
            self.generate_wordcloud(all_text, name=sender_name.lower())

    def generate_wordcloud(self, text, name):
        # Tokenize
        if self.nlp is None:
            print(f'Missing language data for wordcloud. Please run the following command from within your venv: "python -m spacy download {self.lang_code}_core_web_sm"')
            return
        doc = self.nlp(text)
        words = [token.text.lower() for token in doc if self.interesting_pos(token)]
        unique_words = set(words)
        word_counts = Counter(words)
        # Wordcloud
        wc_kwargs = {
            'width': 1200,
            'height': 800,
            'max_words': 200,
        }
        if self.font_path is not None and self.font_path.is_file():
            wc_kwargs |= {'font_path': str(self.font_path)}
        wc = WordCloud(**wc_kwargs).generate_from_frequencies(word_counts)
        wc.to_file(self.output_folder / f'wordcloud-{name}.png')

    # Tokeniation
    very_uninteresting_pos = set([
        'NUM', # numeral
        'PUNCT', # punctuation
        'SYM', # symbol
        'X',  # other
    ])
    uninteresting_pos = set([
        'ADP', # adposition
        'PART', # particle
        'DET', # determiner
        'CCONJ', # coordinating conjunction
        'SCONJ', # subordinating conjunction
        'AUX', # auxiliary
        'PRON', # pronouns
    ])

    def interesting_pos(self, token):
        if token.pos_ in self.very_uninteresting_pos:
            # Filter very uninteresting parts of speech
            return False
        if self.strong_pos_filter and token.pos_ in self.uninteresting_pos:
            # Filter uninteresting parts of speech
            return False
        if token.text in self.custom_word_filter:
            # Filter words from custom filter
            return False
        if len(token.shape_) <= 1:
            # Filter emojis
            return False
        if token.tag_ == '_SP':
            # Filter whitespaces
            return False
        return True

    def debug_tokens(self):
        text = ' '.join(self.df['message'])
        doc = self.nlp(text)
        legend = '\t'.join([
            'text', 'lemma_', 'pos_', 'tag_', 'dep_',
            'shape_', 'is_alpha', 'is_stop'
        ])
        for i, t in enumerate(doc[:1_000]):
            if i % 50 == 0:
                print('='*30)
                print(legend)
                print('='*30)
            print(self.token_summary(t))

    @staticmethod
    def token_summary(token):
        return '\t'.join(str(_) for _ in [
            token.text, token.lemma_, token.pos_, token.tag_, token.dep_,
            token.shape_, token.is_alpha, token.is_stop
        ])


def main():
    arg_space = util.parse_args()
    output_dir = util.resolve_output(arg_space.output,
        clear=arg_space.clear, force_clear=arg_space.force_clear,
        ignore=[Importer.CACHED_DF_NAME],
        )
    if arg_space.show_output:
        util.open_file_explorer(output_dir)
    custom_word_filter = None
    if isinstance(arg_space.custom_word_filter, Path):
        if arg_space.custom_word_filter.is_file():
            t = util.file_load(arg_space.custom_word_filter)
            custom_word_filter = set(t.split())
            print(f'Filtering {len(custom_word_filter)} custom words.')
        else:
            raise FileNotFoundError(f'Cannot find custom word filter file: {arg_space.custom_word_filter}')
    df = Importer(
        import_file=arg_space.file, output_folder=output_dir,
        mode=arg_space.mode, line_limit=arg_space.line_limit,
        anonymize_senders=arg_space.anonymize, cache_data=arg_space.cache_data,
        ).df
    a = Analyzer(df,
        output_folder=output_dir, font_path=arg_space.font_path,
        lang_code=arg_space.lang_code, strong_pos_filter=arg_space.strong_filter,
        custom_word_filter=custom_word_filter,
    )
    a.analyze(analyses=arg_space.analyses)
    a.export_figures()


if __name__ == '__main__':
    main()
