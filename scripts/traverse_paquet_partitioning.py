# MIT License
# Copyright (c) [2021] [Artem Lukoianov]

import os
import json
import argparse

def traverse_paquet_partitioning(root_path, partition_prefix="sym_root="):
    """Generates partition mapping for the specified parquet file

    args:
      root_path: str, path to the root directory of the partitioning
      partition_prefix: str, prefix of the directories of the partitioning
    returns:
      dict: {partition name: partition file path}
    """
    # Traverse the root directory
    dirs = list(filter(lambda x: x.startswith(partition_prefix), os.listdir(root_path)))
    dir_paths = [os.path.join(root_path, x) for x in dirs]

    # Traverse each partitioning directory
    files = [list(filter(lambda x: x.endswith(".parquet"), os.listdir(p)))[0] for p in dir_paths]
    file_paths = [os.path.join(p, n) for p, n in zip(dir_paths, files)]

    # Truncate dir names to extract the 
    partition_name = [x[len(partition_prefix):] for x in dirs]

    return dict(zip(partition_name, file_paths))


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Traverse partitioned parquet and generate mappinpaths.json')
  parser.add_argument(dest='root_path', type=str, help='path to the root of parquet partitioning')
  args = parser.parse_args()

  # Put the output to the same directory with the root_path
  out_path = '.'.join(args.root_path.split('.')[:-1]) + '.json'
  mapping = traverse_paquet_partitioning(args.root_path)

  with open(out_path, "w") as f:
    json.dump(mapping, f)

  print("Mapping is saved to ", out_path)