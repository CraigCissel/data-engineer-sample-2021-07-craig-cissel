from src.csv_utils import read_source_columns, read_source_data, write_data_to_csv
from src.some_storage_library import SomeStorageLibrary


SOURCE_COLUMNS_PATH = 'data/source/SOURCECOLUMNS.txt'
SOURCE_DATA_PATH = 'data/source/SOURCEDATA.txt'
FILE_PATH = 'transformed_data.csv'

if __name__ == '__main__':
    print('Beginning the ETL process...')
    try:
        columns = read_source_columns(SOURCE_COLUMNS_PATH)
        data = read_source_data(SOURCE_DATA_PATH)

        write_data_to_csv(FILE_PATH, columns, data)
       
        storage_library = SomeStorageLibrary()
        storage_library.load_csv(FILE_PATH)
    except FileNotFoundError as e:
        print(f'File not found: {e}')
    except IOError as e:
        print(f'I/O error occurred: {e}')
    except ValueError as e:
        print(f'Value error: {e}')
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
    else:
        print('ETL process completed successfully!')
    finally:
        print('ETL process finished.')
