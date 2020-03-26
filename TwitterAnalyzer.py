# Message Passing Interface (MPI) standard.
from mpi4py import MPI
import os
# Command line argument parser.
import sys, getopt
# Json parser and time calculation.
import json, time

# The constants definition
JSON_LANGUAGES_PROPERTY = "langs"
CMD_LINE_DEFINED_ARGUMENTS = "c:d:"

def parse_arguments(argv):
  # Initialise ariables
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
    if opt in '-c':
      config_path = arg
    elif opt in ("-d"):
       data_path = arg
    
  # Return all arguments
  return config_path, data_path

def load_language_config(file_path):
  if os.path.exists(file_path):
    with open(file_path) as f:
        config_content = json.loads(f.read())
        return config_content[JSON_LANGUAGES_PROPERTY]
  else:
    print("The language configuration file does not exist. Path: %s", file_path)

def process_tweets(rank, data_path, processes):
  if os.path.exists(data_path):
    with open(data_path) as f:
        try:
            for i, line in enumerate(f):
                line = line.replace(",\n","")
        except Exception as error:
            print("Error occurred during processing twitter data file. Error: %s" %error)
  else:
    print("The twitter data file does not exist. Path: %s", data_path)

def gather_processed_data(comm):
    processes = comm.Get_size()

def process_data_master_node(comm, file_path):
    rank = comm.Get_rank()
    size = comm.Get_size()

def process_data_slave_nodes(comm, file_path):
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
          load_language_config(config_path)
          process_data_master_node(comm, data_path)
      else:
          # Perform tasks for slave nodes.
          process_data_slave_nodes(comm, data_path)

# Run the actual program
if __name__ == "__main__":
  star_time = time.time()
  main(sys.argv[1:])
  print("Tottal processing time is : ", str(time.time() - star_time))