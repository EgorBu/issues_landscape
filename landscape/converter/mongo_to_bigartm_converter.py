"""
Convert Github daily issues to one of BigARTM format
(UCI Bag-of-words https://bigartm.readthedocs.io/en/stable/tutorials/datasets.html)
"""
import argparse
from datetime import datetime
import os
from typing import List, Optional

import pymongo
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import CountVectorizer
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from tqdm import tqdm

from landscape.utils import is_between_dates

stopwords = list(STOP_WORDS)
english_lang_model = spacy.load("en_core_web_md", disable=["tagger", "parser", "ner"])
client = pymongo.MongoClient("localhost", 27017)
issues_landscape_db = client["issues_landscape"]


class TokenizedIssue:
    """
    The TokenizedIssue object contains issue id and title
    :param issue_id: Issue id
    :param title: Issue title
    """

    def __init__(self, issue_id, title):
        self.id = issue_id
        self.title = title


def build_corpus_from_dumps(start_date: str, end_date: str, target_dir: str) -> None:
    """
    Build corpus of issues from mongodb dumps from start_date to end_date
    :param start_date: Start date from which issues will be used in corpus
    :param end_date: End date to which issues will be used in corpus
    :param target_dir: Directory where vocabulary and docword files will be placed
    """
    dump_collections = list(filter(lambda collection:
                                   is_between_dates(collection, start_date, end_date,
                                                    filename_pattern="mongo-dump-(.*)_issues"),
                                   issues_landscape_db.collection_names()))
    issues = []
    for cur_collection in tqdm(dump_collections, total=len(dump_collections)):
        cur_issues = get_issues_from_collection(issues_landscape_db.get_collection(cur_collection))
        issues.extend(cur_issues)

    corpus = [cur_issue.title for cur_issue in issues]
    vectorizer = CountVectorizer().fit(corpus)
    bag_of_words = vectorizer.transform(corpus)
    save_to_docword_file(vectorizer, bag_of_words, issues, target_dir)
    save_to_vocabulary_file(vectorizer.get_feature_names(), target_dir)


def get_issues_from_collection(collection: pymongo.collection.Collection) -> List[TokenizedIssue]:
    """
    Get issue's ids and titles from given collection
    :param collection: Mongo collection with issues
    :return: List of tokenized issues
    """
    id_and_title_cursor = extract_fields_from_collection(collection, ["id", "title"])
    return tokenize_issues(id_and_title_cursor)


def extract_fields_from_collection(mongo_collection: pymongo.collection.Collection,
                                   field_names: list) -> pymongo.cursor.Cursor:
    """
    Extract only given field names from collection items
    :param mongo_collection: Collection from which data will be extracted
    :param field_names: Name of fields which will be extracted from collection item
    :return: Cursor which refer to given collection
    """
    field_names_dict = {key: True for key in field_names}
    extracted_fields_cursor = mongo_collection.find({}, field_names_dict)
    return extracted_fields_cursor


def tokenize_issues(id_and_title_cursor: pymongo.cursor.Cursor) -> List[TokenizedIssue]:
    """
    Tokenize issues from mongo db cursor
    :param id_and_title_cursor: Cursor which refer to issues stored in mongodb
    :return: List of tokenized issues
    """
    issues = []
    for cur_doc in tqdm(id_and_title_cursor, total=id_and_title_cursor.count()):
        tokenized_issue = tokenize_issue(cur_doc['title'], 5)
        if tokenized_issue is None:
            continue
        issues.append(TokenizedIssue(cur_doc['id'], tokenized_issue))
    return issues


def tokenize_issue(issue, min_token_number: int) -> Optional[str]:
    """
    Split the issue into tokens
    :param issue: Given issue which will be splitted
    :param min_token_number: Minimal number of tokens in issue
    :return: String with lemmatize tokens if tokens more than min_token_number, otherwise None
    """
    tokenized_issue = english_lang_model(issue.lower())
    result_issue = []
    for word in tokenized_issue:
        if word.is_alpha and word.text in english_lang_model.vocab \
                and word.lemma_ not in stopwords and word.lemma_ != "-PRON-":
            result_issue.append(word.lemma_)
    if len(result_issue) < min_token_number:
        return None
    return " ".join(result_issue)


def save_to_docword_file(vectorizer: CountVectorizer, bag_of_words: csr_matrix,
                         issues: List[TokenizedIssue], target_dir: str) -> None:
    """
    Save words to docword file in following format:
    D (documents number)
    W (words number)
    NNZ (total rows)
    docID wordID count
    docID wordID count
    .....
    :param vectorizer: Count vectorizer
    :param bag_of_words: Matrix where each cell represents number of word appearance in document
    :param issues: Tokenized issues
    :param target_dir: Target directory where docword file will be created
    :return: None
    """
    target_path = os.path.join(target_dir, "docword.issues.txt")
    with open(target_path, "w") as docword_file:
        docword_file.write(str(len(issues)) + "\n")
        docword_file.write(str(len(vectorizer.get_feature_names())) + "\n")
        docword_file.write(str(bag_of_words.nnz) + "\n")
        nnz_x, nnz_y = bag_of_words.nonzero()
        for x, y in zip(nnz_x, nnz_y):
            docword_file.write(
                "%s %s %s\n" % (str(issues[x].id), str(y + 1), str(bag_of_words[x, y])))


def save_to_vocabulary_file(words: List[str], target_dir: str) -> None:
    """
    Save words to vocabulary file in specified target directory
    :param words: Unique vocabulary words
    :param target_dir: Target directory where vocabulary file will be created
    :return: None
    """
    target_path = os.path.join(target_dir, "vocab.issues.txt")
    with open(target_path, "w") as vocab_file:
        for word in words:
            vocab_file.write(word + "\n")


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
    args = parser.parse_args()
    build_corpus_from_dumps(args.start_date, args.end_date, args.target_dir)
