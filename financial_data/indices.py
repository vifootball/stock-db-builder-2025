import pandas as pd
import manual_data.indices_reference
import investpy
import financedatabase as fd

# Sources:
# 1. Yahoo Finance
# 2. FRED (Federal Reserve Economic Data)
# 3. (LATER) investpy
# 4. (LATER) fd (Finance Database)

# L1에서 domain 정보를 넣어줘야함 (stock, index, currency)

def get_indices_masters_yahoo() -> pd.DataFrame:
    world_indices = manual_data.indices_reference.YAHOO_WORLD_INDICES
    world_indices = pd.DataFrame(world_indices)

    comm_indices = manual_data.indices_reference.YAHOO_COMMODITIES
    comm_indices = pd.DataFrame(comm_indices)

    indices = pd.concat([world_indices,comm_indices]).reset_index()
    # indices['domain'] = 'INDEX'
    indices = indices[['symbol', 'name']]
    return indices


def get_indices_masters_fred() -> pd.DataFrame:
    indices = manual_data.indices_reference.FRED
    indices = pd.DataFrame(indices)
    # indices['domain'] = 'INDEX'
    indices = indices[['symbol', 'name']]
    return indices
