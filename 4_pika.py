from module.pik_functions import create_base_df, add_channel_spacing, add_week_and_extension, summary_for_extension
import pandas as pd
import datetime

files = [
    'files/Ericsson_RRL_Capacity_4pika_w2139_w2140_3peak.xlsx',
    'files/Huawei_RRL_Capacity_4pika_pla_w2139_w2140_3peak.xlsx',
    'files/NEC_RRL_Capacity_4pika_w2139_w2140_3peak.xlsx'
]

start = datetime.datetime.now()

print(start)

radiolinks = pd.read_excel('files/Radiolinks.xlsx')
basedf = create_base_df(files)
df_with_channel_spacing = add_channel_spacing(radiolinks, basedf)
week_and_extension = add_week_and_extension(df_with_channel_spacing)
week_and_extension.to_excel('result2.xlsx', index=False)
summary = summary_for_extension(week_and_extension)
summary.to_excel('summary.xlsx', index=False)

end = datetime.datetime.now() - start
print(f'done!')
print(end)
