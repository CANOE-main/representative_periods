"""
Runs clustering and processes all databased to selected representative periods
"""

import database_processing
import clustering
import utils
from matplotlib import pyplot as pp

def run():

    clustering.run()
    database_processing.process_all()

    print("All processing completed.")

    if utils.config['show_plots']:
        print("Showing plots.")
        pp.show()



if __name__ == "__main__":

    run()