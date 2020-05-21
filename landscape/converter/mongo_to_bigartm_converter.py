"""
Convert Github daily issues to one of BigARTM format
(UCI Bag-of-words https://bigartm.readthedocs.io/en/stable/tutorials/datasets.html)
"""
import argparse
from datetime import datetime

import pickle

from landscape.topic_issue_model import TopicIssueModel


def main() -> None:
    """
    Convert daily Github issues from mongodb to BigARTM format (UCI Bag-of-words)
    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--target-dir", required=True,
                        help="Directory to store bigARTM files")
    parser.add_argument("--start-date", default="2015-12-01",
                        help="Start date(YYYY-MM-DD) to convert mongodb files to BigARTM format")
    parser.add_argument("--end-date", default=str(datetime.today().date()),
                        help="End date(YYYY-MM-DD) to convert mongodb files to BigARTM format")
    parser.add_argument("--min-token-number", default=10,
                        help="Minimal number of tokens in tokenized issue")
    parser.add_argument("--min-df", default=5,
                        help="Ignore terms that have a document frequency strictly "
                             "lower than the given threshold (absolute counts)")
    parser.add_argument("--max-df", default=0.5,
                        help="Ignore terms that have a document frequency strictly "
                             "higher than the given threshold (proportion of documents) ")
    args = parser.parse_args()
    topic_issue_model = TopicIssueModel(min_token_number=args.min_token_number, min_df=args.min_df,
                                        max_df=args.max_df, target_dir=args.target_dir)

    corpus = topic_issue_model.build_corpus_from_dumps(args.start_date, args.end_date)
    topic_issue_model.fit(corpus)

    with open("topic_issue_model.pickle", "wb") as issue_pickle_file:
        pickle.dump(topic_issue_model, issue_pickle_file)
