import pandas as pd, numpy as np
import util_Path as UP
import itertools as itt


def clean_multi_index(dataframe: pd.DataFrame):
    null_value = np.nan
    na_rep = '-'
    sep = '|'
    # Rename Unnamed level of columns to nan
    columns_rename = dataframe.columns.to_frame().replace(r'Unnamed: [\w_]+', null_value, regex=True)
    dataframe.columns = pd.MultiIndex.from_frame(columns_rename)

    # Get ID for Unnamed/nan levels, 0,-,2,- means that 0,2 are named, 1,3 are unnamed
    columns_index = columns_rename.copy()
    for e in columns_rename.columns:
        columns_index[e] = columns_rename[e].where(columns_rename[e].isna(), str(e))
    columns_rename[-1] = columns_index.apply(lambda x: x.str.cat(sep=sep, na_rep=na_rep), axis=1)

    # Groupby the Unnamed/nan levels ID
    output = {}
    for k, v in columns_rename.groupby(-1):
        # Get the columns where the ID is applicable
        output[k] = dataframe.reindex(columns=v)
        # Drop the level where it is unnamed
        na_level = [i for i, e in enumerate(k.split(sep)) if e == na_rep]
        v = v.drop(columns=na_level + [-1])
        # Assign to Columns index back to the columns
        try:
            if len(na_level) > 1:
                output[k].columns = v.squeeze()
            else:
                output[k].columns = pd.MultiIndex.from_frame(v)
            print(f'The number of columns of ID "{k}": {output[k].shape[1]}')
        except ValueError:
            print(f'The number of columns where all levels are "Unnamed": {output[k].shape[1]}')
    print()
    return output

def clean_index(dataframe: pd.DataFrame):
    null_value = np.nan
    na_rep = '-'
    sep = '|'
    # Rename Unnamed level of columns to nan
    columns_rename = dataframe.columns.to_series().replace(r'Unnamed: [\w_]+', null_value, regex=True)
    columns_rename = columns_rename[(~columns_rename.isna()) & (columns_rename != null_value)]
    return dataframe.reindex(columns=columns_rename)




