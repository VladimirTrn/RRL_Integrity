from copy import deepcopy
import pandas as pd


def _drop_dublicates(dataframe):
    dataframe = dataframe.drop_duplicates(
        subset=['OriginalDn', 'flag'],
        keep='first'
    )
    return dataframe


def df_to_rows(df):
    rows = []
    for index, row in df.iterrows():
        data_support_row = dict(zip(df.columns, row))
        if data_support_row['flag'] == '01 Traffic':
            for k, v in data_support_row.items():
                if k.isdigit():
                    data_support_row[k] = str(round(v, 2))
        elif data_support_row['flag'] == '02 Utilization All':
            for k, v in data_support_row.items():
                if k.isdigit():
                    data_support_row[k] = str(round(v * 100, 2)) + ' %'
        rows.append(data_support_row)
    return rows


def get_only_need_columns(dataframe: pd.DataFrame) -> list:
    columns = []
    for col in dataframe.columns:
        if col in ['MacroRegion',
                   'RCode2',
                   'OriginalDn',
                   'RRL_NAME',
                   'Оборудование',
                   'FULL_CAPACITY',
                   'NUMBER OF E1s',
                   'E1',
                   'flag'] or col.isdigit():
            columns.append(col)
    return columns


def create_base_df(files):
    result_df = []
    for file in files:
        dataframe = pd.read_excel(file)
        columns = get_only_need_columns(dataframe)
        dataframe = _drop_dublicates(dataframe[columns])
        dataframe = dataframe.rename(columns={'E1': 'NUMBER OF E1s'})
        dataframe = pd.DataFrame(df_to_rows(dataframe))
        result_df.append(dataframe)
        print(f'complete {file}')
    return pd.concat(result_df)


def add_channel_spacing(radiolinks: pd.DataFrame, basedf: pd.DataFrame):
    merge_result_and_radiolinks = pd.merge(basedf,
                                           radiolinks[['OriginalDn', 'Channel Spacing']],
                                           on='OriginalDn',
                                           how='left')
    channel_spacing_column = merge_result_and_radiolinks.columns[-1]
    old_columns = merge_result_and_radiolinks.columns[:-1]
    result_columns = old_columns.insert(7, channel_spacing_column)
    return merge_result_and_radiolinks[result_columns]


def add_week_and_extension(dataframe):
    result = []
    for index, row in dataframe.iterrows():
        data_support_row = dict(zip(dataframe.columns, row))
        for k, v in deepcopy(data_support_row).items():
            if k.isdigit():
                if float(v.replace('%', '')) >= 70:
                    data_support_row['Week'] = k
                    if float(data_support_row['Channel Spacing']) in [28.0]:
                        data_support_row['Extension'] = 'SW'
                    elif float(data_support_row['Channel Spacing']) in [56.0, 112.0]:
                        data_support_row['Extension'] = 'HW'

        result.append(data_support_row)
    result = pd.DataFrame(result)
    columns = list(result.columns[:-2])
    columns.insert(8, 'Week')
    columns.insert(9, 'Extension')
    return result[columns]
