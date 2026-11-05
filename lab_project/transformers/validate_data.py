import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def validate_data(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    df = data.copy()
    df['transaction_ts'] = pd.to_datetime(
        df['transaction_ts']
    )

    df['quantity'] = pd.to_numeric(
        df['quantity']
    )

    df['unit_price'] = pd.to_numeric(
        df['unit_price']
    )

    if df['transaction_id'].isnull().any():
        raise ValueError(
            'transaction_id cannot be NULL'
        )

    return df

