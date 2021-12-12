from module.pik_functions import create_base_df, add_channel_spacing, add_week_and_extension
import pandas as pd
import datetime
import sqlite3
from sqlalchemy import create_engine

files = [
    'files/Ericsson_RRL_Capacity_4pika_w2139_w2140_3peak.xlsx',
    'files/Huawei_RRL_Capacity_4pika_pla_w2139_w2140_3peak.xlsx',
    'files/NEC_RRL_Capacity_4pika_w2139_w2140_3peak.xlsx'
]

start = datetime.datetime.now()
print(start)

# radiolinks = pd.read_excel('files/Radiolinks.xlsx')
# basedf = create_base_df(files)
# df_with_channel_spacing = add_channel_spacing(radiolinks, basedf)
# add_week_and_extension(df_with_channel_spacing).to_excel('result.xlsx', index=False)

r = pd.read_excel("result1.xlsx")
engine = create_engine('sqlite://', echo=False)
with engine.begin() as connection:
    r.to_sql('users', con=connection)
    for i in engine.execute(
        """
SELECT  u.MacroRegion, 
        u.RCode2, 
        u.Week,
        (SELECT COUNT(Extension) as cnt FROM users GROUP BY MacroRegion) as cnt,
        (SELECT COUNT(Extension) as HW FROM users WHERE Extension = 'HW' GROUP BY MacroRegion, Week) as HW, 
        (SELECT COUNT(Extension) as SW FROM users WHERE Extension = 'SW' GROUP BY MacroRegion, Week) as SW 
    FROM users u
    WHERE u.Week != 'NULL'
    ORDER BY u.MacroRegion
""").fetchall():
        print(i)


end = datetime.datetime.now() - start
print(f'done!')
print(end)

"""
SELECT u.MacroRegion, 
        u.RCode2, 
        u.Week,
        u1.cnt,
        u2.HW 
        u3.SW 
    FROM users u, 
        (SELECT MacroRegion, COUNT(Extension) as cnt FROM users GROUP BY MacroRegion) as u1
        (SELECT MacroRegion, COUNT(Extension) as HW FROM users WHERE Extension = 'HW' GROUP BY MacroRegion, Week) as u2
        (SELECT MacroRegion, COUNT(Extension) as SW FROM users WHERE Extension = 'SW' GROUP BY MacroRegion, Week) as u3
    WHERE u.MacroRegion = u1.MacroRegion
    AND u1.MacroRegion = u2.MacroRegion
    AND u2.MacroRegion = u3.MacroRegion
    ORDER BY MacroRegion
    """



