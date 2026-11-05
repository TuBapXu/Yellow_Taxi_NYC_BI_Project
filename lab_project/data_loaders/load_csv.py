import pandas as pd
from pathlib import Path


@data_loader
def load_data_from_file(*args, **kwargs):
    """
    Template for loading data from filesystem.
    Load data from 1 file or multiple file directories.

    For multiple directories, use the following:
        FileIO().load(file_directories=['dir_1', 'dir_2'])

    Docs: https://docs.mage.ai/design/data-loading#fileio
    """
    batch_file = kwargs.get( #Using Runtime Variable or default batch_01.csv
        'batch_file',
        'batch_01.csv',
    )

    file_path = Path(
        '/home/src/data'
    ) / batch_file

    df = pd.read_csv(file_path)

    df['source_file'] = batch_file

    return df

