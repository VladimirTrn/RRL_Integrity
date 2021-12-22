from copy import deepcopy
import pandas as pd
from sqlalchemy import create_engine


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
            if k.isdigit() and '%' in v:
                if float(v.replace('%', '')) >= 70:
                    data_support_row['Week'] = k
                    if float(data_support_row['Channel Spacing']) in [28.0]:
                        data_support_row['Extension'] = 'SW'
                    elif float(data_support_row['Channel Spacing']) in [56.0] \
                            and data_support_row['FULL_CAPACITY'] <= 370:
                        data_support_row['Extension'] = 'SW'
                    elif float(data_support_row['Channel Spacing']) in [56.0, 112.0] \
                            and data_support_row['FULL_CAPACITY'] > 370:
                        data_support_row['Extension'] = 'HW'
                    break

        result.append(data_support_row)
    result = pd.DataFrame(result)
    columns = list(result.columns[:-2])
    columns.insert(8, 'Week')
    columns.insert(9, 'Extension')
    return result[columns]


def summary_for_extension(week_and_extension_dataframe):
    SQL_QUERY = """
SELECT  u.MacroRegion, 
        u.RCode2, 
        u.Week,
        u2.cnt,
        u3.cnt,
        COUNT(u.Extension)
    FROM tempdb u LEFT JOIN  
        (SELECT MacroRegion, RCode2, Week, COUNT(Extension) as cnt
         FROM tempdb
         WHERE Week IS NOT NULL 
         AND Extension = 'HW'
         GROUP BY MacroRegion, RCode2, Week) as u2
    ON u.MacroRegion = u2.MacroRegion
    AND u.RCode2 = u2.RCode2
    AND u.Week = u2.Week
    LEFT JOIN
        (SELECT MacroRegion, RCode2, Week, COUNT(Extension) as cnt
         FROM tempdb 
         WHERE Week IS NOT NULL 
         AND Extension = 'SW'
         GROUP BY MacroRegion, RCode2, Week) as u3
    ON u2.MacroRegion = u3.MacroRegion
    AND u2.RCode2 = u3.RCode2
    AND u2.Week = u3.Week
    WHERE u.Week IS NOT NULL 
    AND u.Extension IS NOT NULL 
    GROUP BY u.MacroRegion, u.RCode2, u.Week
    ORDER BY u.MacroRegion
"""
    engine = create_engine('sqlite://', echo=False)
    with engine.begin() as connection:
        week_and_extension_dataframe.to_sql('tempdb', con=connection)
        rows = engine.execute(SQL_QUERY).fetchall()
        return pd.DataFrame(rows, columns=['MacroRegion', 'RCode2', 'Week', 'HW', 'SW', 'HW_SW'])