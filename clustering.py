import pandas as pd
import matplotlib.pyplot as pp
import tsam.timeseriesaggregation as tsam
import utils
import os

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
out_data = this_dir + "clustering_output_data/"

initialised = False



def init():

    global initialised

    if initialised: return

    if not os.path.isdir(out_data): os.mkdir(out_data)
        
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

    df_timeseries = collect_timeseries()

    print("Clustering over timeseries:\n")

    print(df_timeseries)

    test_periods = set(utils.config['test_periods']) if utils.config['test_periods'] is not None else set()
    test_periods.add(utils.config['final_periods']) # in case it wasn't already in the set
    test_periods = list(test_periods)
    test_periods.sort()

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

    colour = [1, 0, 0]
    for n_periods in test_periods:

        df_predicted = cluster_days(df_timeseries=df_timeseries, n_periods=n_periods)

        for ts in df_timeseries.columns:
            if n_periods == utils.config['final_periods']:
                df_predicted[ts].sort_values(ascending=False).reset_index(drop=True).plot(label=f"*{n_periods} periods", ax=dur_axes[ts], lw=2, color=(0, 0.8, 0))
                df_predicted[ts].reset_index(drop=True).plot(label=f"*{n_periods} periods", ax=ts_axes[ts], color='red')
            else:
                df_predicted[ts].sort_values(ascending=False).reset_index(drop=True).plot(label=f"{n_periods} periods", ax=dur_axes[ts], color=tuple(colour))

        if len(test_periods) <= 1: break
        if colour[2] < 1: colour[2] = min(1, colour[2] + 2/(len(test_periods)-1))
        else: colour[0] = colour[0] = max(0, colour[0] - 2/(len(test_periods)-1))
            
    for ts in df_timeseries.columns:
        dur_axes[ts].legend()
        dur_figs[ts].savefig(out_data + f"duration_curve_plots/{ts}.pdf")
        ts_axes[ts].legend()
        ts_figs[ts].savefig(out_data + f"timeseries_plots/{ts}.pdf")

    print("\nClustering complete.")

    if show_plots:
        print("Showing plots.")
        pp.show()


def cluster_days(df_timeseries: pd.DataFrame, n_periods: int) -> pd.DataFrame:

    method = utils.config['clustering_method']
    csv_name = f"{method}_{n_periods}p.csv"

    print(f"\nClustering {n_periods} periods using {method} method...")

    if utils.config['force_days'] is None: forced_periods = []
    else:
        forced_days = [day + utils.config['day_to_index'] for day in utils.config['force_days']]
        forced_periods = [day // utils.config['days_per_period'] for day in forced_days]

    ts_agg = tsam.TimeSeriesAggregation(
        df_timeseries,
        noTypicalPeriods = n_periods - len(forced_periods),
        hoursPerPeriod = 24*utils.config['days_per_period'],
        clusterMethod = method,
        extremePeriodMethod='new_cluster_center',
        addManual=forced_periods,
        resolution=1,
        solver='gurobi',
    )

    weights = ts_agg.clusterPeriodNoOccur
    indices = ts_agg.clusterCenterIndices + forced_periods
    days = [index_to_season(d) for d in indices]

    df_days = pd.DataFrame(index=days, data=weights.values(), columns=['weight']).sort_index()
    df_days.to_csv(out_data + "representative_periods/" + csv_name)

    if n_periods == utils.config['final_periods']:
        print("\nOutput representative periods:\n")
        print(df_days.head(50))

    if n_periods == utils.config['final_periods']: df_days.to_csv(this_dir + "periods.csv")

    df_typ_periods = ts_agg.createTypicalPeriods()
    df_typ_periods.index = df_typ_periods.index.set_levels(df_typ_periods.index.levels[0].map(lambda i: days[i]), level=0)
    df_typ_periods = df_typ_periods.sort_index(level=0)
    df_typ_periods.to_csv(out_data + "reduced_timeseries/" + csv_name)

    df_accuracy = ts_agg.accuracyIndicators()
    df_accuracy.to_csv(out_data + "accuracy_indicators/" + csv_name)

    df_predicted = ts_agg.predictOriginalData()
    df_predicted.to_csv(out_data + "recreated_timeseries/" + csv_name)
    
    return df_predicted



def index_to_season(idx: int):

    day = index_to_day(idx)

    if utils.config['days_per_period'] == 1: return utils.stringify_day(day)
    elif utils.config['days_per_period'] > 1:
        day_2 = day + utils.config['days_per_period'] - 1

        return f"{utils.stringify_day(day)}-{utils.stringify_day(day_2)}"


def index_to_day(idx: int):
    d = idx * utils.config['days_per_period'] - utils.config['day_to_index']
    return d



# Collects all selected timeseries and puts them into a dataframe
def collect_timeseries():

    dfs = []
    files = get_all_files() # gets a list of paths to csv files

    for path in files:

        file = this_dir + "/".join(path) + '.csv' # turn path list into actual file path
        df = pd.read_csv(file, index_col=0).astype(float)
        df.index = range(len(df.index))
        dfs.append(df) # read the csv and add to pile

    # Concatenate all found csv files into a single dataframe for TSAM
    df_timeseries = pd.concat(dfs, axis='columns')
    return df_timeseries



# Walks the timeseries nested dictionary to find all selected csv files
def get_all_files() -> list[list[str]]:

    files = []
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