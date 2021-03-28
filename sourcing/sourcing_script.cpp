// MIT License
// Copyright (c) [2021] [Artem Lukoianov]

#include <vector>
#include <string>
#include <cstdint>
#include <iostream>
#include <fstream>

#include <nlohmann/json.hpp>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

using json = nlohmann::json;
using arrow::Int64Builder;
using arrow::StringBuilder;

std::shared_ptr<arrow::Table> read_parquet_table(const std::string& path) {
  std::shared_ptr<arrow::io::ReadableFile> infile;
  PARQUET_ASSIGN_OR_THROW(
      infile,
      arrow::io::ReadableFile::Open(path, arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  std::shared_ptr<arrow::Table> table;
  PARQUET_THROW_NOT_OK(reader->ReadTable(&table));

  return table;
}

void compute_most_traded_stocks(json partition_mapping) {
  std::map<std::string, int> volume_counter;
  for (auto& partition_path : partition_mapping) {
    std::shared_ptr<arrow::Table> table = read_parquet_table(partition_path);

    auto ids =
      std::static_pointer_cast<arrow::StringArray>(table->GetColumnByName("sym_suffix")->chunk(0));
    auto n_trades =
      std::static_pointer_cast<arrow::DoubleArray>(table->GetColumnByName("n_trades")->chunk(0));

    for (int64_t i = 0; i < table->num_rows(); i++) {
      std::string id = ids->GetString(i);
      int64_t num = n_trades->Value(i);

      std::cout << id << " : " << num << std::endl;

      typename std::map<std::string, int>::const_iterator it = volume_counter.find( id );
      if ( it == volume_counter.end() ) {
          volume_counter[id] = 0;
      }

      volume_counter[id] += num;
    }
  }

  for (const auto& [key, value] : volume_counter) {
    std::cout << key << " = " << value << "\n ";
  }
}

int main(int argc, char** argv) {

  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " file_path" << std::endl;
    return EXIT_FAILURE;
  }

  json partition_mapping;
  std::ifstream inp_file(argv[1]);
  inp_file >> partition_mapping;

  compute_most_traded_stocks(partition_mapping);

  // std::shared_ptr<arrow::Table> table = read_parquet_table(argv[1]);
  // std::cout << "Loaded table (" << 
  //   table->num_rows() << " x " << table->num_columns() << ")" << std::endl;

  return EXIT_SUCCESS;
}
