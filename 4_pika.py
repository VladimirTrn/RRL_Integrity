import pandas as pd
import datetime


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


files = [
    'files/Ericsson_RRL_Capacity_4pika_w2139_w2140_3peak.xlsx',
    'files/Huawei_RRL_Capacity_4pika_pla_w2139_w2140_3peak.xlsx',
    'files/NEC_RRL_Capacity_4pika_w2139_w2140_3peak.xlsx'
]

start = datetime.datetime.now()
print(start)
result_df = []
for file in files:
    dataframe = pd.read_excel(file)
    columns = get_only_need_columns(dataframe)
    dataframe = _drop_dublicates(dataframe[columns])
    dataframe = dataframe.rename(columns={'E1': 'NUMBER OF E1s'})
    dataframe = pd.DataFrame(df_to_rows(dataframe))
    result_df.append(dataframe)
    print(f'complete {file}')


radiolinks = pd.read_excel('files/Radiolinks.xlsx')

result = pd.concat(result_df)

merge_result_and_radiolinks = pd.merge(result, radiolinks[['OriginalDn', 'Channel Spacing']], on='OriginalDn', how='left')
channel_spacing_column = merge_result_and_radiolinks.columns[-1]
old_columns = merge_result_and_radiolinks.columns[:-1]
result_columns = old_columns.insert(7, channel_spacing_column)
merge_result_and_radiolinks[result_columns].to_excel('result.xlsx', index=False)

end = datetime.datetime.now() - start
print(f'done!')
print(end)
