#! /usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import os

import ggplot
import pandas as pd

from parsers import config


def data_check():
    """
    Ensure that there's literally anyting in the /raw/ directory besides .gitkeep.

    Prompt the user with a friendly reminder if there isn't.
    """

    repo_root = os.path.abspath(os.path.pardir)
    data_dir = os.path.join(repo_root, '', 'raw')

    if os.listdir(data_dir) < 2:
        sys.exit(
            "No messages found. Please copy your messages into the 'raw' directory."
        )

    # now that that check is here we don't need to make data a required argument.
    # we know for a fact that there are files in the correct directory, and so we'll parse everything in there.


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d',
        '--data',
        dest='data_paths',
        nargs='+',
        help='chat log data files (pickle files)',
    )
    parser.add_argument(
        '--plot-density',
        dest='density',
        action='store_true',
        help='plots the message densities (KDE) instead of their count')
    parser.add_argument(
        '-n',
        '--number-senders',
        dest='top_n',
        type=int,
        default=10,
        help=
        'number of different senders to consider, ordered by number of messages sent'
    )
    parser.add_argument(
        '-b',
        '--bin-width',
        dest='bin_width',
        type=int,
        default=25,
        help='bin width for histograms')
    parser.add_argument(
        '--filter-conversation',
        dest='filter_conversation',
        type=str,
        default=None,
        help='only keep messages sent in a conversation with this sender')
    parser.add_argument(
        '--filter-sender',
        dest='filter_sender',
        type=str,
        default=None,
        help='only keep messages sent by this sender')
    parser.add_argument(
        '--remove-sender',
        dest='remove_sender',
        type=str,
        default=None,
        help='remove messages sent by this sender')
    args = parser.parse_args()
    return args


def load_data(data_paths,
              filter_conversation=None,
              filter_sender=None,
              remove_sender=None,
              top_n=10):
    # data loading
    df = pd.DataFrame()
    for data_path in data_paths:
        print('Loading', data_path, '...')
        df = pd.concat([df, pd.read_pickle(data_path)])

    df.columns = config.ALL_COLUMNS
    print('Loaded', len(df), 'messages')

    # filtering
    if filter_conversation is not None:
        df = df[df['conversationWithName'] == filter_conversation]

    if filter_sender is not None:
        df = df[df['senderName'] == filter_sender]

    if remove_sender is not None:
        df = df[df['senderName'] != remove_sender]

    merged = merge_frame(df)
    # I separated everything below because:
    # 1 i wanted to limit the scope of the load_data function
    # 2 the load_data functions in analyse and cloud now mirror each other exactly. you could actually delete this entire function in one file and simply import it from the other OR store the return values as some serrialized data and simply work with it as needed instead of running the same code twice.

    return merged


def merge_frame(df):
    # keep only topN interlocutors
    mf = df.groupby(['conversationWithName'], as_index=False) \
        .agg(lambda x: len(x)) \
        .sort_values('timestamp', ascending=False)['conversationWithName'] \
        .head(top_n).to_frame()

    print(mf)

    merged = pd.merge(df, mf, on=['conversationWithName'], how='inner')
    merged = merged[['datetime', 'conversationWithName', 'senderName']]

    print('Number to render:', len(merged))
    print(merged.head())
    return merged


def render(data, bin_width, plot_density=False):
    if plot_density:
        plot = ggplot.ggplot(data, ggplot.aes(x='datetime', color='conversationWithName')) \
               + ggplot.geom_density() \
               + ggplot.scale_x_date(labels='%b %Y') \
               + ggplot.ggtitle('Conversation Densities') \
               + ggplot.ylab('Density') \
               + ggplot.xlab('Date')
    else:
        plot = ggplot.ggplot(data, ggplot.aes(x='datetime', fill='conversationWithName')) \
               + ggplot.geom_histogram(alpha=0.6, binwidth=bin_width) \
               + ggplot.scale_x_date(labels='%b %Y', breaks='6 months') \
               + ggplot.ggtitle('Message Breakdown') \
               + ggplot.ylab('Number of Messages') \
               + ggplot.xlab('Date')

    print(plot)


def main():
    data_check()
    args = parse_arguments()
    data = load_data(
        data_paths=args.data_paths,
        filter_conversation=args.filter_conversation,
        filter_sender=args.filter_sender,
        remove_sender=args.remove_sender,
        top_n=args.top_n,
    )
    # I think it would do a lot of good to serialize this data variable. Then at the top of the file, run a check to see whether our serialized data exists. If it does, simply rerender the analysis instead of going through all of  the text processing again.
    render(data, bin_width=args.bin_width, plot_density=args.density)


if __name__ == '__main__':
    main()
