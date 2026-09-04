"""
Runs clustering and processes all databased to selected representative periods
"""

from matplotlib import pyplot as pp

import clustering
import database_processing
import database_processing_v3
import database_processing_v3_1
import database_processing_v4
import utils


def run():

    clustering.run()  # cluster periods
    database_processing.process_all()  # process Temoa 2 databases
    database_processing_v3.process_all()  # process Temoa 3 databases
    database_processing_v3_1.process_all()  # process Temoa 3.1 databases
    database_processing_v4.process_all()  # process Temoa/CANOE schema 4.0 databases

    print("All processing completed.")

    if utils.config["show_plots"]:
        print("Showing plots.")
        pp.show()


if __name__ == "__main__":
    run()
