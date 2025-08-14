import pandas as pd
import matplotlib.pyplot as pp
import feature_selection as fi
import tsam.timeseriesaggregation as tsam
import utils
import os

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
out_data = this_dir + "clustering_output_data/"

initialised = False



def init():

    global initialised # safety! only do this once

    if initialised: return

    if not os.path.isdir(out_data): os.mkdir(out_data)
    
    ## Make output data directories or clear them out
    out_dirs = [
        'reduced_timeseries/',
        'accuracy_indicators/',
        'recreated_timeseries/',
        'representative_periods/',
        'duration_curve_plots/',
        'timeseries_plots/'
    ]

    for dir in out_dirs:
        if not os.path.isdir(out_data + dir): os.mkdir(out_data + dir)

        # Empty output data directories
        for file in os.listdir(out_data + dir):
            try: os.remove(out_data + dir + file)
            except Exception as e: print(e)

    initialised = True
    print("\nInitialised clustering.\n")



def run(show_plots=False):

    init()

    # Get selected timeseries to cluster over
    df_timeseries = collect_timeseries()

    print("Clustering over timeseries:\n")

    print(df_timeseries) # display chosen timeseries for error catching

    # Get the list of test numbers of periods for plotting
    test_periods = set(utils.config['test_periods']) if utils.config['test_periods'] is not None else set()
    test_periods.add(utils.config['final_periods']) # in case it wasn't already in the set
    test_periods = list(test_periods)
    test_periods.sort()

    # Build figures and plot original timeseries and duration curves
    dur_axes = dict()
    dur_figs = dict()
    ts_axes = dict()
    ts_figs = dict()
    for ts in df_timeseries.columns:
        dur_figs[ts], dur_axes[ts] = pp.subplots(figsize = [10, 6], dpi = 100, nrows = 1, ncols = 1)
        df_timeseries[ts].sort_values(ascending=False).reset_index(drop=True).plot(label='original', lw=3, style='k-', ax=dur_axes[ts])
        dur_axes[ts].set_title(f"duration curve of original {ts} and weighted representative periods")
        dur_axes[ts].set_xlabel('duration (h)')
        dur_axes[ts].set_ylabel(ts)

        ts_figs[ts], ts_axes[ts] = pp.subplots(figsize = [10, 6], dpi = 100, nrows = 1, ncols = 1)
        df_timeseries[ts].reset_index(drop=True).plot(label='original', lw=2, style='b-', ax=ts_axes[ts])
        ts_axes[ts].set_title(f"timeseries of original {ts} and weighted representative periods")
        ts_axes[ts].set_xlabel('duration (h)')
        ts_axes[ts].set_ylabel(ts)

    # Plot each set of test periods on both figures going from red -> blue with increasing n periods. Green if final number of periods
    colour = [1, 0, 0]
    for n_periods in test_periods:

        df_predicted = cluster_days(df_timeseries=df_timeseries, n_periods=n_periods)
        if df_predicted is None: continue

        for ts in df_timeseries.columns:
            if n_periods == utils.config['final_periods']:
                df_predicted[ts].sort_values(ascending=False).reset_index(drop=True).plot(label=f"*{n_periods} periods", ax=dur_axes[ts], lw=2, color=(0, 0.8, 0))
                df_predicted[ts].reset_index(drop=True).plot(label=f"*{n_periods} periods", ax=ts_axes[ts], color='red')
            else:
                df_predicted[ts].sort_values(ascending=False).reset_index(drop=True).plot(label=f"{n_periods} periods", ax=dur_axes[ts], color=tuple(colour))

        # This transitions linearly from red to blue
        if len(test_periods) <= 1: break
        if colour[2] < 1: colour[2] = min(1, colour[2] + 2/(len(test_periods)-1))
        else: colour[0] = colour[0] = max(0, colour[0] - 2/(len(test_periods)-1))
    
    # Add the legend and save the figure to output data directory
    for ts in df_timeseries.columns:
        dur_axes[ts].legend()
        dur_figs[ts].savefig(out_data + f"duration_curve_plots/{ts}.pdf")
        ts_axes[ts].legend()
        ts_figs[ts].savefig(out_data + f"timeseries_plots/{ts}.pdf")

    print("\nClustering complete.\n")

    if show_plots:
        print("Showing plots.")
        pp.show()



def cluster_days(df_timeseries: pd.DataFrame, n_periods: int) -> pd.DataFrame:

    method = utils.config['clustering_method']
    csv_name = f"{method}_{n_periods}p.csv"

    print(f"\nClustering {n_periods} periods using {method} method...\n")

    # Get any configured forced days, make index conversion and convert to period indices (if multiday periods)
    if utils.config['force_days'] is None: forced_periods = []
    else:
        forced_days = [day + utils.config['day_to_index'] for day in utils.config['force_days']]
        forced_periods = [day // utils.config['days_per_period'] for day in forced_days] # does nothing if one-day periods

    # Collect custom feature periods based on any configured in the list
    forced_periods.extend([p for p in collect_custom_feature_periods() if p not in forced_periods])

    # Making room for extreme periods
    extreme_periods = utils.config['extreme_periods']
    n_clusters = n_periods - len(forced_periods) - sum(len(val) for val in extreme_periods.values() if val)

    if n_clusters < 1:
        print("Too many feature periods! Nothing left for clustering. Skipping.")
        return

    # Execute the clustering
    ts_agg = tsam.TimeSeriesAggregation(
        df_timeseries,
        noTypicalPeriods = n_clusters,
        hoursPerPeriod = 24*utils.config['days_per_period'],
        clusterMethod = method,
        extremePeriodMethod='new_cluster_center',
        addManual=forced_periods,
        addPeakMax=extreme_periods['max_peak'],
        addPeakMin=extreme_periods['min_peak'],
        addMeanMax=extreme_periods['max_mean'],
        addMeanMin=extreme_periods['min_mean'],
        resolution=1,
        solver='gurobi',
    )

    # Get the indices of chosen periods and their weights of the year
    weights = ts_agg.clusterPeriodNoOccur
    indices = ts_agg.clusterCenterIndices

    if len(ts_agg.extremePeriods.values()) < n_periods - n_clusters:
        print(f"Overlap between feature periods! Lost {n_periods - n_clusters - len(ts_agg.extremePeriods.values())} period(s).")

    # Add feature period indices
    if n_periods == utils.config['final_periods']: print("Selected feature periods:")
    for name, period in ts_agg.extremePeriods.items():
        index = period['stepNo']
        if n_periods == utils.config['final_periods']: print(utils.index_to_season(index), name)
        if index not in indices: indices.append(index)
        else: print(f"Feature period {utils.index_to_season(index)} overlapped with typical periods! Lost one period.")
    
    # Convert period indices to string period names as in the database
    days = [utils.index_to_season(d) for d in indices]

    # Saving day selection and weights for all test numbers of periods to output data directory
    df_days = pd.DataFrame(index=days, data=weights.values(), columns=['weight']).sort_index()
    df_days.to_csv(out_data + "representative_periods/" + csv_name)

    # For the final number of periods, output to periods.csv for database processing
    if n_periods == utils.config['final_periods']:
        print("\nOutput representative periods:\n")
        print(df_days.head(50), '\n')
        df_days.to_csv(this_dir + "periods.csv")

        # Also output the representative sequence
        sequence = [utils.index_to_season(ts_agg.clusterCenterIndices[i]) for i in ts_agg.clusterOrder]
        df_sequence = pd.DataFrame(index=range(len(sequence)), data=sequence, columns=['period'])
        df_sequence.to_csv(this_dir + "sequence.csv")

    # Output the timeseries data for the periods selected
    df_typ_periods = ts_agg.createTypicalPeriods()
    df_typ_periods.index = df_typ_periods.index.set_levels(df_typ_periods.index.levels[0].map(lambda i: days[i]), level=0)
    df_typ_periods = df_typ_periods.sort_index(level=0)
    df_typ_periods.to_csv(out_data + "reduced_timeseries/" + csv_name)

    # Output accuracy indicators for clustering
    df_accuracy = ts_agg.accuracyIndicators()
    df_accuracy.to_csv(out_data + "accuracy_indicators/" + csv_name)

    # Output recreated full-length timeseries based on selected periods
    df_predicted = ts_agg.predictOriginalData()
    df_predicted.to_csv(out_data + "recreated_timeseries/" + csv_name)
    
    return df_predicted



# Collects all selected timeseries and puts them into a dataframe for clustering
def collect_timeseries() -> pd.DataFrame:

    dfs = []
    files = get_all_files() # gets a list of paths to selected timeseries csv files

    for path in files:

        file = this_dir + "/".join(path) + '.csv' # turn path list into actual file path
        df = pd.read_csv(file, index_col=0).astype(float)
        df.index = range(len(df.index))
        dfs.append(df) # read the csv and add to the list

    # Concatenate all found csv files into a single dataframe for TSAM
    df_timeseries = pd.concat(dfs, axis='columns')
    return df_timeseries



# Collects and returns custom feature periods identified by feature identification based on list configured in config
def collect_custom_feature_periods() -> list[int]:

    custom_feature_periods = []

    if utils.config['custom_features'] is None: return custom_feature_periods

    # For each feature configured in the list, pass that dictionary to the relevant
    # feature identification method and get period indices in return
    for feature in utils.config['custom_features']:
            
            match feature['method']:
                case 'max_mean_period': custom_feature_periods.extend(fi.max_mean_period(feature))
    
    return custom_feature_periods



# Walks the timeseries nested dictionary to find all selected csv files
def get_all_files() -> list[list[str]]:

    files = [] # just a collector that the recursive method can add into
    get_files(['timeseries'], utils.config['timeseries'], files)
    
    return files

# Recursively walks a dictionary to find more dictionaries or, otherwise, a list of files
def get_files(dir: list, dictionary: dict, files):

    for key, value in dictionary.items():

        _dir = dir.copy()
        _dir.append(key)

        # Found a nested dictionary, go one level deeper
        if isinstance(value, dict):
            get_files(_dir, dictionary[key], files) # si chiama a se stesso

        # Found a list of files. Append them to the files list
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str): files.append([*_dir, item])



if __name__ == "__main__":

    init()
    run(show_plots=utils.config['show_plots'])