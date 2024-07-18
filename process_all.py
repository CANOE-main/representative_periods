"""
Runs clustering and processes all databased to selected representative periods
"""

import database_processing
import database_processing_v3
import clustering
import utils
from matplotlib import pyplot as pp

def run():

    clustering.run() # cluster periods
    database_processing.process_all() # process Temoa 2 databases
    database_processing_v3.process_all() # process Temoa 3 databases

    print("All processing completed.")

    if utils.config['show_plots']:
        print("Showing plots.")
        pp.show()



if __name__ == "__main__":

    run()