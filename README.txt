A package for processing CANOE databases to apply representative periods. Currently configured for original Temoa schemas.

=============
clustering.py
=============
Generates representative periods based on configuration in config.yaml. Clustering data is output to clustering_output_data/. Representative periods are saved to periods.csv.

======================
database_processing.py
======================
Applies representative periods in periods.csv to all sqlite databases in input_sqlite/, outputting processed databases to output_sqlite/.

==============
process_all.py
==============
Runs clustering.py then database_processing.py.

========================
timeseriesaggregation.py
========================
This file must be replaced into the tsam python library for these scripts to work. Found somewhere like
C:\Users\<user>\miniconda3\Lib\site-packages\tsam\
or
C:\Users\<user>\miniconda3\envs\<env>\Lib\site-packages\tsam\