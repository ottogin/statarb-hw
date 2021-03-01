// MIT License
// Copyright (c) [2021] [Artem Lukoianov]

#include <vector>
#include <string>
#include <cstdint>
#include <iostream>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

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

int main(int argc, char** argv) {

  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " file_path" << std::endl;
    return EXIT_FAILURE;
  }
 
  std::shared_ptr<arrow::Table> table = read_parquet_table(argv[1]);

  std::cout << "Loaded table (" << 
    table->num_rows() << " x " << table->num_columns() << ")" << std::endl;

  return EXIT_SUCCESS;
}
