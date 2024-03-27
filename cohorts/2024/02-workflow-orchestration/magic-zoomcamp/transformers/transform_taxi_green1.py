import pandas as pd
import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
 

@transformer
def transform(data, *args, **kwargs):
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
    # Specify your transformation logic here
    print(f"Preprocessing: row with zero passengers: {data['passenger_count'].isin([0]).sum()}")

    #Remove row with 0 passenger count & trip distance
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    # Add a new column by converting lpep_pickup_datetime to date
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date 

    # CamelCase to snake_case 
    vendor_ids = data['VendorID'].unique()
    print(vendor_ids)   
    data.columns = (data.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
             ) 

    return data
 


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns, "There is no vendor_id in columns"

    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'

    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance'
