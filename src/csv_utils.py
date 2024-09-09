"""Module providing utility functions for csv reads and writes."""

import csv
from typing import List

def read_source_columns(source_columns_path: str) -> List[str]:
    """reads columns from source path and returns them as a list ordered by their index"""
    column_definitions = []
    try:
        with open(source_columns_path, 'r') as file:
            for line in file:
                column_definition = _parse_column_definition(line)
                if column_definition:
                    column_definitions.append(column_definition)
    except FileNotFoundError as e:
        print(f"Error: The file '{source_columns_path}' was not found.")
        raise e
    except IOError as e:
        print(f"Error: An I/O error occurred while reading the file '{source_columns_path}': {e}")
        raise e
    sorted_columns = _sort_columns_by_index(column_definitions)
    return _extract_column_names(sorted_columns)

def _parse_column_definition(line: str) -> List[tuple]:
    """Parses a single line from the column definitions file into a tuple (index, column_name)."""  
    split_line = line.strip().split('|')
    if len(split_line) != 2:
        raise ValueError(f"Incorrect format: Expected 'index|column_name', but got: '{line.strip()}'")
    try:
        index = int(split_line[0])
        column_name = split_line[1]
        return index, column_name
    except ValueError as e:
        print(f"ValueError: {e} Unable to convert index to integer. Line: '{line.strip()}'")
        raise

def _sort_columns_by_index(columns: List[tuple]) -> List[tuple]:
    """ Sorts columns by their index."""
    return sorted(columns, key=lambda x: x[0])

def _extract_column_names(sorted_columns: List[tuple]) -> List[str]:
    """Extracts column names from a list of sorted column tuples."""
    return [column_name for _, column_name in sorted_columns]

def read_source_data(source_data_path: str) -> List[List[str]]:
    """Function reads source data from a specified path."""
    data = []
    try:
        with open(source_data_path, 'r') as file:
            for line in file:
                values = line.strip().split('|')
                data.append(values)
    except FileNotFoundError as e:
        print(f"Error: The file '{source_data_path}' was not found.")
        raise e
    except IOError as e:
        print(f"Error: An I/O error occurred while reading the file '{source_data_path}': {e}")
        raise e
    return data

def write_data_to_csv(file_path: str, columns: str, data: str) -> None:
    """Function writes the columns and headers to a csv in the desired path."""
    try:
        with open(file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            writer.writerows(data)
    except IOError as e:
        print(f"Error: Unable to write to file '{file_path}'. {e}")
        raise e
