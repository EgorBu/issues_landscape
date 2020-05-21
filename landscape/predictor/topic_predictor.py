import argparse

import artm
import pandas
import pickle

from landscape.topic_issue_model import TopicIssueModel


def predict_topics(topic_issue_model: TopicIssueModel, model_artm: artm.artm_model.ARTM) -> None:
    """
    Predict topics for given issue in input. To stop predicting, enter the word "stop"
    :param topic_issue_model: Model with fitting CountVectorizer
    :param model_artm: Trained bigARTM model
    :return: None
    """
    issue = input()
    while issue != "stop":
        try:
            theta_matrix = topic_issue_model.predict(issue=issue, model_artm=model_artm)
            print_top3_topics(theta_matrix, model_artm)
        except (ValueError, Exception) as exc:
            print(exc)
        issue = input()


def print_top3_topics(theta_matrix: pandas.DataFrame, model_artm: artm.artm_model.ARTM) -> None:
    """
    Print the 3 most likely topics from given dataframe
    :param theta_matrix: Pandas dataframe with topics and their probability for given issue
    :param model_artm: Trained bigARTM model
    :return: None
    """
    top3_topics = theta_matrix[theta_matrix.columns[0]].nlargest(3)
    topic_names = list(top3_topics.index)
    for i, topic_name in enumerate(topic_names):
        print(topic_name, top3_topics[i],
              model_artm.score_tracker["TopTokensScore"].last_tokens[topic_name])


def main() -> None:
    """
    Predict topics for given issue in input
    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-artm", required=True,
                        help="Path to directory with BigARTM model")
    args = parser.parse_args()
    model_artm = artm.load_artm_model(args.model_artm)

    with open('topic_issue_model.pickle', 'rb') as issue_pickle_file:
        topic_issue_model: TopicIssueModel = pickle.load(issue_pickle_file)
    predict_topics(topic_issue_model, model_artm)
