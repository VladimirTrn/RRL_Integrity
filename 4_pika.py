from module.pik_functions import create_base_df, add_channel_spacing, add_week_and_extension, summary_for_extension, \
    df_type_identification
import pandas as pd
import pathlib
import datetime


base_path = pathlib.Path(__file__).parent.joinpath('files')
files = [str(_) for _ in base_path.iterdir() if str(_).endswith('peak.xlsx')]


start = datetime.datetime.now()

print(start)

basedf = create_base_df(files)
radiolinks = pd.read_excel('files/Radiolinks.xlsx')
df_with_channel_spacing = add_channel_spacing(radiolinks, basedf)
df_with_type_identification = df_type_identification(df_with_channel_spacing)
df_with_type_identification.to_excel('result_with_types.xlsx', index=False)
week_and_extension = add_week_and_extension(df_with_type_identification)
week_and_extension.to_excel('result3.xlsx', index=False)
summary = summary_for_extension(week_and_extension)
summary.to_excel('summary.xlsx', index=False)

end = datetime.datetime.now() - start
print(f'done!')
print(end)
