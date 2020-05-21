from collections import namedtuple
import os
from typing import List

import artm
import pandas
import pymongo
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import CountVectorizer
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from tqdm import tqdm

from landscape.utils import is_between_dates

client = pymongo.MongoClient("localhost", 27017)
issues_landscape_db = client["issues_landscape"]
TokenizedIssue = namedtuple("TokenizedIssue", ["id", "title"])
english_lang_model = spacy.load("en_core_web_md", disable=["tagger", "parser", "ner"])


class TopicIssueModel:
    """
        The TopicIssueModel object contains min_token_number, target_dir and CountVectorizer
        This class is responsible for building corpus from dumps, saving corpus in BigARTM format
        and predicting topic for giving request
        :param min_token_number: Minimal number of tokens in tokenized issue
        :param min_df: Ignore terms that have a document frequency strictly lower
        than the given threshold (absolute counts)
        :param max_df: Ignore terms that have a document frequency strictly higher
        than the given threshold (proportion of documents)
        :param target_dir: Directory to store bigARTM files
        """

    def __init__(self, min_token_number: int, min_df: int, max_df: float, target_dir: str):
        self.min_token_number = min_token_number
        self.count_vectorizer = CountVectorizer(min_df=min_df, max_df=max_df)
        self.target_dir = target_dir

    def build_corpus_from_dumps(self, start_date: str, end_date: str) -> List[TokenizedIssue]:
        """
        Build corpus of issues from mongodb dumps from start_date to end_date
        :param start_date: Start date from which issues will be used in corpus
        :param end_date: End date to which issues will be used in corpus
        :return: List of tokenized issues
        """
        dump_collections = list(filter(lambda collection:
                                       is_between_dates(collection, start_date, end_date,
                                                        filename_pattern="mongo-dump-(.*)_issues"),
                                       issues_landscape_db.list_collection_names()))
        issues = []
        for collection in tqdm(dump_collections, total=len(dump_collections)):
            cur_issues = self.__get_issues_from_collection(issues_landscape_db.
                                                         get_collection(collection))
            issues.extend(cur_issues)
        return issues

    def __get_issues_from_collection(self, collection: pymongo.collection.Collection) \
            -> List[TokenizedIssue]:
        """
        Get issue's ids and titles from given collection
        :param collection: Mongo collection with issues
        :return: List of tokenized issues
        """
        id_and_title_cursor = self.__extract_fields_from_collection(collection, ["id", "title"])
        return self.__tokenize_issues(id_and_title_cursor)

    @staticmethod
    def __extract_fields_from_collection(mongo_collection: pymongo.collection.Collection,
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

    def __tokenize_issues(self, id_and_title_cursor: pymongo.cursor.Cursor) -> List[TokenizedIssue]:
        """
        Tokenize issues from mongo db cursor. Skip issues which cannot be tokenized
        :param id_and_title_cursor: Cursor which refer to issues stored in mongodb
        :return: List of tokenized issues
        """
        issues = []
        for cur_doc in tqdm(id_and_title_cursor, total=id_and_title_cursor.count()):
            try:
                tokenized_issue = self.__tokenize_issue(cur_doc["title"])
            except:
                continue
            issues.append(TokenizedIssue(cur_doc["id"], tokenized_issue))
        return issues

    def __tokenize_issue(self, issue: str) -> str:
        """
        Split the issue into tokens
        :param issue: Given issue which will be splitted
        :return: String with lemmatize tokens if tokens more than min_token_number,
        otherwise raise Exception
        """
        tokenized_issue = english_lang_model(issue.lower())
        result_issue = []
        for word in tokenized_issue:
            if word.is_alpha and word.text in english_lang_model.vocab \
                    and word.lemma_ not in STOP_WORDS and word.lemma_ != "-PRON-":
                result_issue.append(word.lemma_)
        if len(result_issue) < self.min_token_number:
            raise Exception("Not enough tokens in your request")
        return " ".join(result_issue)

    def fit(self, issues: List[TokenizedIssue]) -> None:
        """
        Vectorize given issues with CountVectorizer and save it in BigARTM format (Bag-of-words)
        :param issues: Corpus of tokenized issues
        :return: None
        """
        issues_titles = [issue.title for issue in issues]
        bag_of_words = self.count_vectorizer.fit_transform(issues_titles)
        self.__save_to_bigartm_format(bag_of_words, issues, self.target_dir)

    def predict(self, issue: str, model_artm: artm.artm_model.ARTM) -> pandas.DataFrame:
        """
        Predict topics for given issue (or another set of key words)
        :param issue: Issue or set of key words
        :param model_artm: Trained bigARTM model
        :return: Pandas dataframe with topics and their probability for given issue
        """
        tokenized_issue = self.__tokenize_issue(issue)
        bag_of_words = self.count_vectorizer.transform([tokenized_issue])
        predict_dir = os.path.join(self.target_dir, "predict")
        self.__save_to_bigartm_format(bag_of_words, [TokenizedIssue(1, tokenized_issue)],
                                    predict_dir)
        batch_vectorizer = artm.BatchVectorizer(data_path=predict_dir,
                                                data_format="bow_uci",
                                                collection_name="issues",
                                                target_folder=os.path.join(predict_dir,
                                                                           "batch_predict"))
        return model_artm.transform(batch_vectorizer)

    def __save_to_bigartm_format(self, bag_of_words: csr_matrix, issues: List[TokenizedIssue],
                               target_dir: str) -> None:
        """
        Save issue's bag-of-words to BigARTM format
        :param bag_of_words: Matrix where each cell represents number of word appearance in document
        :param issues: Tokenized issues
        :param target_dir: Target directory where BigARTM files will be created
        :return: None
        """
        self.__save_to_docword_file(bag_of_words, issues, target_dir)
        self.__save_to_vocabulary_file(self.count_vectorizer.get_feature_names(), target_dir)

    def __save_to_docword_file(self, bag_of_words: csr_matrix,
                             issues: List[TokenizedIssue], target_dir: str) -> None:
        """
        Save words to docword file in following format:
        D (documents number)
        W (words number)
        NNZ (total rows)
        docID wordID count
        docID wordID count
        .....
        :param bag_of_words: Matrix where each cell represents number of word appearance in document
        :param issues: Tokenized issues
        :param target_dir: Target directory where docword file will be created
        :return: None
        """
        target_path = os.path.join(target_dir, "docword.issues.txt")
        with open(target_path, "w") as docword_file:
            docword_file.write(str(len(issues)) + "\n")
            docword_file.write(str(len(self.count_vectorizer.get_feature_names())) + "\n")
            docword_file.write(str(bag_of_words.nnz) + "\n")
            nnz_x, nnz_y = bag_of_words.nonzero()
            for x, y in zip(nnz_x, nnz_y):
                docword_file.write(
                    "%s %s %s\n" % (str(issues[x].id), str(y + 1), str(bag_of_words[x, y])))

    @staticmethod
    def __save_to_vocabulary_file(words: List[str], target_dir: str) -> None:
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
