# MIT License
# Copyright (c) [2021] [Artem Lukoianov]

import time
import random
import argparse
import numpy as np
import pandas as pd
from pyarrow import parquet


class TradesLookUpTable:
    """Class to quikly access stocks for a given time interval

    Created for a single purpose - serve queries of
        get_stocks_in_the_interval on the market data

    Args:
        table: pd.DataFrame - dataframe describing the trades
    """
    def __init__(self, table):
        self.data = table[['minute', 'id']].groupby(["minute"])["id"].unique()

    def get_stocks_in_the_interval(self, interval):
        """Returns a list of stocks traded in a given interval INCLUDING both ends.

        Args:
            interval: tuple(int, int) - start and end points of the time interval

        Returns:
            list, stocks' names traded in the given time interval
        """
        data_slice = self.data[(self.data.index >= interval[0]) &
                               (self.data.index <= interval[1])].reset_index()['id']
        return np.unique(np.concatenate(data_slice))

def read_market_data(path):
    """Reads and verifies market data from a specified parquet file/partitioning.

    Args:
        path: str, path to parquet file or partitioning with market data

    Returns:
        pd.DataFrame, loaded data
    """

    dataset = parquet.ParquetDataset(path)
    table = dataset.read().to_pandas()
    
    ## Check that data contains all neccesary fields
    needed_fields = ["date", "sym_root", "sym_suffix", "close", "size"]
    for field_name in needed_fields:
        assert field_name in table.columns,\
            f"Field <%s> is required, but not present in the data" % field_name

    ## Concatenate sym_suffix and sym_root to get unique identifier of each stock
    sym_suffix_str = table['sym_suffix'].fillna("").astype(str)
    table["id"] = table['sym_root'].astype(str) + "." + sym_suffix_str

    return table
    
def get_k_most_traded_stocks(table, k=20):
    """Returns names of top k stocks sorted by total size.

    Args:
        table: pd.DataFrame - dataframe describing the trades
        k:     int - number of top stocks to return

    Returns:
        pd.Series, sorted total sizes of top k stocks
    """

    sorted_stocks = table.groupby(by="id")["size"].sum().sort_values(ascending=False)
    return sorted_stocks[:k]

def compute_volume_weighted_prices(table, volumes):
    """Computes VWAP of all measurments in the table and for each stock in volumes.index.

    Args:
        table: pd.DataFrame - dataframe describing daily trades of stocks
        volumes: pd.Series - total volume of trades for each stock we want to compute VWAP for

    Returns:
        pd.Series, volume weighted average prices for each stocks
    """

    # Drop all rows that we don't need to include into VWAP
    truncated_table = table[table.id.isin(top_k_stocks.index)].copy()

    truncated_table["volume_times_price"] = truncated_table["size"] * truncated_table["price_av"]
    volume_price_sums = truncated_table.groupby(by="id")["volume_times_price"].sum()
    return volume_price_sums / volumes

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Compute statistics on a given market data')
  parser.add_argument(dest='path', type=str, help='path to market data in parquet format')
  parser.add_argument('-n', type=int, default=10, help='number of runs for each function for averaging')
  args = parser.parse_args()

  # 0. Read the table
  start_t = time.time()
  table = read_market_data(args.path)
  print("Data is read in %.4fs" % (time.time() - start_t))

  # 1. Compute total volume for top 20 stocks
  start_t = time.time()
  for _ in range(args.n):
    top_k_stocks = get_k_most_traded_stocks(table)
  print("Total volume is computed in %.4fs averaged by %druns" % 
                    ((time.time() - start_t) / args.n, args.n))
  print(top_k_stocks, "\n\n")
  
  # 2. Compute Volume-Weighted Average Price
  start_t = time.time()
  for _ in range(args.n):
    wvap = compute_volume_weighted_prices(table, top_k_stocks)
  print("Volume-Weighted Average is computed in %.4fs averaged by %d runs" %  
                    ((time.time() - start_t) / args.n, args.n))
  print(wvap, "\n\n")

  # 3. Get stocks in interval
  start_t = time.time()
  look_table = TradesLookUpTable(table)
  print("Lookup table is creates in %.4fs" % (time.time() - start_t))

  start_t = time.time()
  for _ in range(args.n):
    b = random.randint(0, 396)
    a = random.randint(0, b)
    traded_stocks = look_table.get_stocks_in_the_interval((a, b))
  print("Traded stocks are found in %.4fs averaged by %d randomized runs" % 
                    ((time.time() - start_t) / args.n, args.n))
