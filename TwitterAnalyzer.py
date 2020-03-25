# Message Passing Interface (MPI) standard.
from mpi4py import MPI
# Command line argument parser.
import getopt
# Json parser and time calculation. 
import json, time

def parse_arguments(argv):
  # Initialise ariables
  config_path = ""
  data_path = ""

  ## Try to parse command line arguments
  try:
    opts, args = getopt.getopt(argv,"cd:")
  except getopt.GetoptError as error:
    sys.exit(2)
  for opt, arg in opts:
    if opt in '-c':
      config_path = arg
    elif opt in ("-d"):
       data_path = arg
    
  # Return all arguments
  return config_path, data_path

def load_language_config():
    with open("") as f:
        config_content = json.load(f.read())

def process_tweets(rank, data_path, processes):
    with open(data_path) as f:
        try:
            for i, line in enumerate(f):
                line = line.replace(",\n","")
        except Exception:
            print("Erorr occurred during processing twitter data file.")        

def marshall_tweets(comm):
    processes = comm.Get_size()         

def master_tweet_processor(comm, file_path):
    rank = comm.Get_rank()
    size = comm.Get_size()

def slave_tweet_processor(comm, file_path):
    rank = comm.Get_rank()
    size = comm.Get_size()

def main(args):
    # Get the main communicator
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    # Parse command line arguments to get language configuration and twitter data files.
    config_path, data_path = parse_arguments(args)

    if rank == 0 :
        # Perform tasks for the master node.
        load_language_config(config_path)
        master_tweet_processor(comm, data_path)
        
    else:
        # Perform tasks for slave nodes.
        slave_tweet_processor(comm, data_path)

# Run the actual program
if __name__ == "__main__":
  star_time = time.time()
  main()
  print("Tottal processing time is : ", str(time.time() - star_time))