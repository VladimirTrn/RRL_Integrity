from copy import deepcopy
import pandas as pd
from sqlalchemy import create_engine
import logging
import os
import patoolib
import pyodbc

logging.basicConfig(level=logging.INFO,
                    # filename='app.log',
                    # filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s')


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
                   'CAPACITY',
                   'NUMBER OF E1s',
                   'E1',
                   'flag'] or col.isdigit():
            columns.append(col)
    return columns


def create_base_df(files):
    result_df = []
    logging.info(f'Объединяем файлы:')
    for file in files:
        dataframe = pd.read_excel(file)
        columns = get_only_need_columns(dataframe)
        dataframe = _drop_dublicates(dataframe[columns])
        dataframe = dataframe.rename(columns={'E1': 'NUMBER OF E1s'})
        dataframe = pd.DataFrame(df_to_rows(dataframe))
        result_df.append(dataframe)
        logging.info(f'complete {file}')
    return pd.concat(result_df)


def add_channel_spacing(radiolinks: pd.DataFrame, basedf: pd.DataFrame):
    logging.info(f'Добавляем данные из radiolinks...')
    # columns = ['OriginalDn',
    #            'Base TX Frequency',
    #            'xpiccalculated',
    #            'Map Length',
    #            'Existencestate',
    #            ]
    columns = ['OriginalDn',
               'Base TX Frequency',
               'xpiccalculated',
               'Map Length',
               'Существующее состояние',
               ]
    merge_result_and_radiolinks = pd.merge(basedf,
                                           radiolinks[columns],
                                           on='OriginalDn',
                                           how='left')
    channel_spacing_column = merge_result_and_radiolinks.columns[-1]
    old_columns = merge_result_and_radiolinks.columns[:-1]
    result_columns = old_columns.insert(7, channel_spacing_column)
    return merge_result_and_radiolinks[result_columns]


def df_type_identification(dataframe):
    logging.info(f'Определяем тип пролета...')
    temp = []
    for index, row in deepcopy(dataframe).iterrows():
        data_row = dict(zip(dataframe.columns, row))

        if str(data_row['FULL_CAPACITY']) == 'nan' or data_row['FULL_CAPACITY'] == 0:
            data_row['FULL_CAPACITY'] = data_row['CAPACITY']

        if data_row['FULL_CAPACITY'] <= 495 and str(data_row['xpiccalculated']) == 'nan' and data_row[
            'Base TX Frequency'] < 70000:
            data_row['Type'] = '1+0'
        elif str(data_row['xpiccalculated']) != 'nan':
            data_row['Type'] = 'XPIC/2+0'
        elif data_row['Base TX Frequency'] >= 70000:
            data_row['Type'] = 'E-band'
        else:
            data_row['Type'] = 'None'

        logging.debug(f'Type = {data_row["Type"]}')
        logging.debug(data_row)

        temp.append(data_row)
    result = pd.DataFrame(temp)
    columns = [_ for _ in result.columns if _ != 'CAPACITY']
    c = columns[-4:]
    columns = columns[:8] + c + columns[8:-4]
    return result[columns]


def add_week_and_extension(dataframe):
    logging.info(f'Определяем неделю и расширение...')
    result = []
    temp_row = None
    for index, row in dataframe.iterrows():
        data_support_row = dict(zip(dataframe.columns, row))
        if data_support_row['flag'] == '01 Traffic':
            temp_row = data_support_row
            result.append(data_support_row)
            continue
        else:
            for k, v in deepcopy(data_support_row).items():
                if k.isdigit() and '%' in v:
                    if float(v.replace('%', '')) >= 70:
                        data_support_row['Week'] = k
                        E1 = temp_row['NUMBER OF E1s'] if str(temp_row['NUMBER OF E1s']) != 'nan' else 0
                        formula = (float(temp_row[k]) * 100 / 60) + E1 * 2
                        if data_support_row['Type'] == '1+0':
                            if formula < 350:
                                data_support_row['Extension'] = 'SW'
                            elif formula >= 350 \
                                    and temp_row['Map Length'] <= 3500:
                                data_support_row['Extension'] = 'HW(Eband)'
                            else:
                                data_support_row['Extension'] = 'HW'
                        elif data_support_row['Type'] == 'XPIC/2+0':
                            if formula < 750:
                                data_support_row['Extension'] = 'SW'
                            elif formula >= 750 \
                                    and temp_row['Map Length'] <= 3500:
                                data_support_row['Extension'] = 'HW(Eband)'
                            elif formula >= 750 \
                                    and temp_row['Map Length'] >= 3500:
                                data_support_row['Extension'] = 'HWXPIC/rerout'
                            else:
                                data_support_row['Extension'] = 'SW'
                        elif data_support_row['Type'] == 'E-band':
                            data_support_row['Extension'] = 'SW'
                        elif data_support_row['Type'] == 'None':
                            data_support_row['Extension'] = 'WTF'
                        logging.debug(f"Extension = {data_support_row['Extension']}")
                        logging.debug(temp_row)
                        logging.debug(data_support_row)
                        break

        result.append(data_support_row)
    result = pd.DataFrame(result)
    columns = list(result.columns[:-6])
    columns.insert(8, 'Week')
    columns.insert(9, 'Extension')
    return result[columns]


def extension_for_hw_plan(week_and_extension_dataframe):
    SQL_QUERY = """
    SELECT RCode2 as region, count(Extension) as HW_Plan
    FROM tempdb
    WHERE Extension IN ('HW(Eband)', 'HW', 'WTF', 'HWXPIC/rerout')
    GROUP BY RCode2
    """
    engine = create_engine('sqlite://', echo=False)
    with engine.begin() as connection:
        week_and_extension_dataframe.to_sql('tempdb', con=connection)
        rows = engine.execute(SQL_QUERY).fetchall()
        return pd.DataFrame(rows, columns=['region', 'HW Plan'])


def summary_for_extension_HW(week_and_extension_dataframe):
    SQL_QUERY = """
SELECT  u.MacroRegion, 
        u.RCode2, 
        u.Week,
        u2.cnt
        -- u3.cnt,
        -- COUNT(u.Extension)
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
        # return pd.DataFrame(rows, columns=['MacroRegion', 'RCode2', 'Week', 'HW', 'SW', 'HW_SW'])
        return pd.DataFrame(rows, columns=['MacroRegion', 'RCode2', 'Week', 'HW'])




def summary_for_extension_SW(week_and_extension_dataframe):
    SQL_QUERY = """
SELECT  u.MacroRegion, 
        u.RCode2, 
        u.Week,
        u2.cnt
        -- u3.cnt,
        -- COUNT(u.Extension)
    FROM tempdb u LEFT JOIN  
        (SELECT MacroRegion, RCode2, Week, COUNT(Extension) as cnt
         FROM tempdb
         WHERE Week IS NOT NULL 
         AND Extension = 'SW'
         GROUP BY MacroRegion, RCode2, Week) as u2
    ON u.MacroRegion = u2.MacroRegion
    AND u.RCode2 = u2.RCode2
    AND u.Week = u2.Week
    LEFT JOIN
        (SELECT MacroRegion, RCode2, Week, COUNT(Extension) as cnt
         FROM tempdb 
         WHERE Week IS NOT NULL 
         AND Extension = 'HW'
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
        return pd.DataFrame(rows, columns=['MacroRegion', 'RCode2', 'Week', 'SW'])

def set_hw_for_all_hw_extension(week_and_extension):
    week_and_extension.loc[week_and_extension['Extension'] == 'WTF', 'Extension'] = 'HW'
    week_and_extension.loc[week_and_extension['Extension'] == 'HWXPIC/rerout', 'Extension'] = 'HW'
    week_and_extension.loc[week_and_extension['Extension'] == 'HW(Eband)', 'Extension'] = 'HW'
    return week_and_extension

def split_by_years(df_with_type_identification):
    logging.info('Разбиваем и получаем файлы с неделями по годам.')
    weeks = []
    others_columns = []
    for column in df_with_type_identification.columns:
        if column.isdigit():
            weeks.append(column)
        else:
            others_columns.append(column)

    years = []
    temp = []
    for index in range(len(weeks)):
        try:
            week1 = weeks[index][:2]
            week2 = weeks[index + 1][:2]
            if week1 == week2:
                temp.append(weeks[index])
            else:
                temp.append(weeks[index])
                years.append(deepcopy(temp))
                temp = []
        except IndexError:
            temp.append(weeks[index])
            years.append(deepcopy(temp))
            temp = []

    result_columns = []
    for year in years:
        new_columns = deepcopy(others_columns)
        new_columns.extend(year)
        result_columns.append(new_columns)

    return result_columns


def get_path_hw(file_name):
    """ Получение последнего файла с пролетами из папки """

    path_user = "//corp.tele2.ru//cpfolders//STAT.CP.Reports//TCH_for_Transport"
    list_files_sw = [s for s in os.listdir(path_user)
                     if os.path.isfile(os.path.join(path_user, s)) and file_name in s]
    list_files_sw.sort(key=lambda s: os.path.getmtime(os.path.join(path_user, s)))
    path_file = path_user + "//" + list_files_sw[-1]

    return path_file


def unpack_file_any(path):
    """ Распаковка любого архива """

    dir_out = "L:\Transport_planning\VISIO ЧТП\Access\Operation Group\Отчеты RRL Capacity_4pica\Weekly_RRL_Integrity"
    patoolib.extract_archive(path,
                             outdir=dir_out,
                             interactive=False
                             )

    return f"{dir_out}\\{path.split('//')[-1].split('.')[0]}.xlsx".replace('//', '\\')


# print(unpack_file_any(get_path_hw('Huawei_RRL_Capacity_4pika_pla')))

class ConnectSql:
    def __init__(self, database=None, request=None):
        self.connect_db = pyodbc.connect("DRIVER={SQL Server};"
                                         "SERVER=TIS-SQL-CLU-A.corp.tele2.ru;"
                                         f"Database={database};"
                                         "PORT=1433;"
                                         "UID=accessnetwork;"
                                         "PWD=xvd5c;"
                                         )
        self.request = request

    def get_cursor(self):
        return self.connect_db.cursor()

    def execute_request(self):
        return self.get_cursor().execute(self.request)


def det_table_sql(str_sql):
    sql = ConnectSql("Reports", str_sql)
    df_sql = pd.read_sql(str_sql, sql.connect_db)
    sql.connect_db.close()
    return df_sql

# df = det_table_sql(
#     (
#         f"SELECT * "
#         f"FROM [Reports_Tele2].[dbo].[ReportRadiolinks]"
#     )
# )
#
# df.to_excel('test.xlsx')
# print(df)
