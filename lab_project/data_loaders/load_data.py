import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def hello(*args, **kwargs):
    """
    Template for loading data from API
    """
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Carol']
    })

    return df


