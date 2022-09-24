from module.pik_functions import unpack_file_any, create_base_df, add_channel_spacing, add_week_and_extension, \
    summary_for_extension, \
    df_type_identification, extension_for_hw_plan, get_path_hw, det_table_sql, split_by_years
import pandas as pd
import pathlib
import datetime
import logging

base_path = pathlib.Path(__file__).parent.joinpath('new_files')
files = [str(_) for _ in base_path.iterdir() if str(_).endswith('peak.xlsx')]

start = datetime.datetime.now()

logging.info(start)
# files = [unpack_file_any(get_path_hw('Huawei_RRL_Capacity_4pika_pla')),
#          unpack_file_any(get_path_hw('Ericsson_RRL_Capacity_4pika')),
#          unpack_file_any(get_path_hw('NEC_RRL_Capacity_4pika'))]
basedf = create_base_df(files)
radiolinks = pd.read_excel('new_files/Radiolinks.xlsx')
# radiolinks = det_table_sql(
#     (
#         f"SELECT * "
#         f"FROM [Reports_Tele2].[dbo].[ReportRadiolinks]"
#     )
# )
df_with_channel_spacing = add_channel_spacing(radiolinks, basedf)
df_with_type_identification = df_type_identification(df_with_channel_spacing)
count = 1
for year in split_by_years(df_with_type_identification):
    df_by_year = add_week_and_extension(df_with_type_identification[year])
    file_name = f'result_by_year_{count}.xlsx'
    logging.info(f'Записываем в {file_name}')
    df_by_year.to_excel(file_name, index=False)
    count += 1

df_with_type_identification.to_excel('full_description.xlsx', index=False)
week_and_extension = add_week_and_extension(df_with_type_identification)
week_and_extension.to_excel('result_full.xlsx', index=False)
# for_alex_dataframe = extension_for_hw_plan(week_and_extension)
# for_alex_dataframe.to_excel('for_alex.xlsx', index=False)
# summary = summary_for_extension(week_and_extension)
# summary.to_excel('summary.xlsx', index=False)

end = datetime.datetime.now() - start
logging.info('done!')
logging.info(end)
