from module.pik_functions import create_base_df, add_channel_spacing, add_week_and_extension, summary_for_extension
import pandas as pd
import pathlib
import datetime


base_path = pathlib.Path(__file__).parent.joinpath('files')
files = [str(_) for _ in base_path.iterdir() if str(_).endswith('peak.xlsx')]


start = datetime.datetime.now()

print(start)

radiolinks = pd.read_excel('files/Radiolinks.xlsx')
basedf = create_base_df(files)
rrliface_speed = pd.read_excel('files/RRL_integrity_w2208.xlsx', sheet_name='Detail_Data')
df_with_channel_spacing = add_channel_spacing(radiolinks, basedf, rrliface_speed)
week_and_extension = add_week_and_extension(df_with_channel_spacing)
week_and_extension.to_excel('result2.xlsx', index=False)
summary = summary_for_extension(week_and_extension)
summary.to_excel('summary.xlsx', index=False)

end = datetime.datetime.now() - start
print(f'done!')
print(end)
