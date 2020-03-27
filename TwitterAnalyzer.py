# Message Passing Interface (MPI) standard.
from mpi4py import MPI
# Counting hashable objects.
from collections import Counter
# Regular expression operations
import re
# Portable way of using system dependent functionality.
import os
# Command line argument parser.
import sys, getopt
# Json parser and time calculation.
import json, time

# The constants definition
FILE_ENCODING = "utf8"
LANG_UNDEFINED = "Undefined"

HASH_TAG_REX = r"#(\w+)"

JSON_DOCUMENT = "doc"
JSON_TEXT_PROPERTY = "text"
JSON_LANGUAGES_PROPERTY = "langs"
JSON_LANGUAGE_PROPERTY = "lang"
JSON_NEW_LINE_STRING = ",\n"

CMD_LINE_DEFINED_ARGUMENTS = "c:d:"
LANG_CONFIG_ARGUMENT = "-c"
TWEET_DATA_ARGUMENT = "-d"

def parse_arguments(argv):
    # Initialise local variables
    config_path = ""
    data_path = ""

    # Try to parse command line arguments
    try:
        opts, args = getopt.getopt(argv, CMD_LINE_DEFINED_ARGUMENTS)
    except getopt.GetoptError as error:
        print("Failed to parse comand line arguments. Error: %s" %error)
        sys.exit(2)
        
    # Extract argument values
    for opt, arg in opts:
        if opt in (LANG_CONFIG_ARGUMENT):
            config_path = arg
        elif opt in (TWEET_DATA_ARGUMENT):
            data_path = arg
        
    # Return all arguments
    return config_path, data_path

def load_language_config(file_path):
    if os.path.exists(file_path):
        with open(file_path) as fstream:
            try:
                config_content = json.loads(fstream.read())
                return config_content[JSON_LANGUAGES_PROPERTY]
            except Exception as exception:
                print("Error occurred during loading language configuration. Exception: %s" %exception)
    else:
        print("The language configuration file does not exist. Path: %s", file_path)

def print_analysis_result(hashtag_counter, lang_counter, lang_config):
    # Print top 10 hashtags
    print("\nTop 10 most commonly used hashtags:")
    for i, hashtag in enumerate(hashtag_counter):
        print("%d. #%s, %d" %(i + 1, hashtag[0], hashtag[1]))

    # Print top 10 languages
    print("\nTop 10 most commonly used languages:")
    for i, lang in enumerate(lang_counter):
        lang_name = ""
        lang_code = lang[0];

        if lang_code in lang_config:
            lang_name = lang_config[lang_code]
        else:
            lang_name = LANG_UNDEFINED

        print("%d. %s (%s), %d" %(i + 1, lang_name, lang_code, lang[1]))

def analyze_tweet(text, language):
    hashtag_counter = None
    lang_counter = None

    if (text):
        # Find all hashtags in the text content
        hashtags = re.findall(HASH_TAG_REX, text)
        # Build counter for hashtags
        hashtag_counter = Counter(hashtags)

    if (language):
        # Build counter for languages
        lang_counter = Counter([language])

    # Return all counters
    return hashtag_counter, lang_counter

def process_twitter_data(rank, data_path, processor_size):
    # Initialise all counters
    total_hashtag_counter = Counter([])
    total_lang_counter = Counter([])

    if os.path.exists(data_path):
        with open(data_path, encoding=FILE_ENCODING) as fstream:
            try:
                for i, line in enumerate(fstream):
                    if (i % processor_size == rank):
                        if (i > 0):
                            line = line.replace(JSON_NEW_LINE_STRING,"")
                            try:
                                # Load tweet into json document
                                tweet = json.loads(line)
                                # Extract text and language properties
                                text = tweet[JSON_DOCUMENT][JSON_TEXT_PROPERTY]
                                language = tweet[JSON_DOCUMENT][JSON_LANGUAGE_PROPERTY]
                                
                                # Analyze tweet data
                                hashtag_counter, lang_counter = analyze_tweet(text, language)
                                
                                # Update to total counters
                                total_hashtag_counter += hashtag_counter
                                total_lang_counter += lang_counter
                            except ValueError as error:
                                print("Failed to decode JSON content from [%d] rank. Error: %s" %(rank, error))
                                print("Processed tweet: %s" %line)
                        else:
                            print("Ignore header line.")
            except Exception as exception:
                print("Error occurred during processing twitter data from [%d] rank. Exception: %s" %(rank, exception))
    else:
        print("The twitter data file does not exist. Path: %s", data_path)

    # Return processed counters
    return total_hashtag_counter, total_lang_counter

def gather_results_all_nodes(comm):
    processes = comm.Get_size()

def perform_tasks_master_node(comm, file_path):
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Extract hashtag and language from tweet data
    hashtag_counter, lang_counter = process_twitter_data(rank, file_path, size)

    # Return all counters
    return hashtag_counter, lang_counter

def perform_tasks_slave_nodes(comm, file_path):
    rank = comm.Get_rank()
    size = comm.Get_size()

def main(args):
    # Get the main communicator
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    # Parse command line arguments to get language configuration and twitter data files.
    config_path, data_path = parse_arguments(args)

    if (not data_path):
        print("The twitter data file path is not specified in command line arguments.")
    else:
        if rank == 0 :
            if (not config_path):
                print("The language configuration file path is not specified in command line arguments.")
            else:
                # Perform tasks for the master node.
                lang_config = load_language_config(config_path)
                hashtag_counter, lang_counter = perform_tasks_master_node(comm, data_path)
                print_analysis_result(hashtag_counter.most_common(10), lang_counter.most_common(10), lang_config)
        else:
            # Perform tasks for slave nodes.
            perform_tasks_slave_nodes(comm, data_path)

# Run the actual program
if __name__ == "__main__":
    star_time = time.time()
    main(sys.argv[1:])
    print("\nTottal processing time is : ", str(time.time() - star_time))