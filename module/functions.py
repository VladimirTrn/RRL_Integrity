import pandas
from styleframe import StyleFrame, Styler


def get_all_tasks_from_excel(file: str) -> pandas.DataFrame:
    dataframe = pandas.read_excel(file)

    def set_project_type(dataframe: pandas.DataFrame) -> str:
        if dataframe['Имя задачи'] in [
            'Сформируй доп соглашение/Отправь в ЭДА',
            'Получи оборудование',
            'Выполни СМР/Сконфигурируй оборудование',
            'Устранить проблему по РРЛ',
            'Выполни работы по удалению потоков'
        ] and dataframe['Ответственное подразделение'] == 'Р':
            return 'Модернизация РРЛ (HW)'

        elif dataframe['Имя задачи'] == 'Ожидание поставки оборудование на ЦС' \
                and dataframe['Ответственное подразделение'] == 'Отдел сетей доступа ТС ЦП':
            return 'Ожидание поставки на ЦС'

        elif dataframe['Имя задачи'] == 'Выполни перемаршрутизацию траффика' \
                and dataframe['Ответственное подразделение'] == 'МР':
            return 'Изменение топологии'

        else:
            return 'None'

    dataframe.loc[:, 'Имя'] = dataframe.apply(set_project_type, axis=1)
    return dataframe


def get_only_need_columns(dataframe: pandas.DataFrame) -> list:
    columns = []
    for col in dataframe.columns:
        if col not in ['Текущая утилизация, %', 'Утилизация на момент  выдачи задания(запуска процесса), %']:
            columns.append(col)
    return columns


def get_supported_tasks(base_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    columns = get_only_need_columns(base_dataframe)
    tasks = base_dataframe[columns].loc[
        (~base_dataframe['Статус задачи'].isin(['Завершенные', 'Отмененные'])) &
        (base_dataframe['Имя'] != "None")]
    return tasks


def get_finished_tasks(base_dataframe: pandas.DataFrame) -> pandas.DataFrame:
    def num_week_filter(data_frame: pandas.DataFrame) -> int:
        result = []
        for x in data_frame['№ недели закрытия задачи']:
            if str(x) != 'nan':
                result.append(int(x))
        return max(result)

    columns = get_only_need_columns(base_dataframe)
    last_week = num_week_filter(base_dataframe)
    tasks = base_dataframe[columns].loc[
        (~base_dataframe['Статус задачи'].isin([
            'Новые',
            'Переадресованные',
            'Поддерживаемые',
            'Отмененные'
        ])) &
        (base_dataframe['Имя задачи'].isin([
            'Выполни перемаршрутизацию траффика',
            'Выполни СМР/Сконфигурируй оборудование',
            'Выполни работы по удалению потоков',
            'Устранить проблему по РРЛ'
        ])) &
        (base_dataframe['Имя'] != "None") &
        (base_dataframe['№ недели закрытия задачи'] == last_week)
        ]
    return tasks


def stylize_and_write(dataframe: pandas.DataFrame, filename: str):
    styled_dataframe = StyleFrame(dataframe)
    segoe = 'Segoe UI'
    grey = '666666'
    light_blue = '00FFFF'
    row_font_size = 10
    header_font_size = 11
    columns = dataframe.columns
    for col in columns:
        if col in [
            'Имя элемента',
            'Имя',
            'Тип задачи',
            'Макрорегион',
            'Ответственное подразделение',
            'Задача принята/выполнена',
            'Регион',
            'Код Закрытия'
        ]:
            width = 30
        elif col == 'Имя задачи':
            width = 45
        else:
            width = 18
        styled_dataframe.apply_column_style(
            cols_to_style=[col],
            styler_obj=Styler(
                font=segoe,
                font_size=row_font_size
            ), width=width)

    styled_dataframe.apply_headers_style(
        cols_to_style=columns,
        styler_obj=Styler(
            font=segoe,
            bold=True,
            font_color=grey,
            font_size=header_font_size,
            bg_color=light_blue)
    )

    ew = StyleFrame.ExcelWriter(filename)
    styled_dataframe.to_excel(ew)
    ew.save()


def get_tasks_from_region(file: str) -> list:
    tasks = pandas.read_excel(file)
    tasks = tasks.rename(columns={1: 'Оборудование'})
    all_rows = []
    for index, row in tasks.iterrows():
        data_row = dict(zip(tasks.columns, row))
        if str(data_row['ЦП']) == 'nan':
            all_rows.append(data_row)

    return all_rows


def merge_region_and_finished_tasks(finished_tasks: pandas.DataFrame,
                                    tasks_from_region: list) -> list:
    filter = ('Макрорегион', set([_['Макрорегион'] for _ in tasks_from_region]))
    finished_rows = df_to_rows(filter, finished_tasks)
    result = []
    for finished_row in finished_rows:
        for region_row in tasks_from_region:
            if finished_row['Process ODn'] == region_row['Process ODn']:
                region_row['ЦП'] = finished_row['№ недели закрытия задачи']
                result.append(region_row)

        if finished_row['Process ODn'] not in [_['Process ODn'] for _ in tasks_from_region]:
            finished_row['ЦП'] = finished_row['№ недели закрытия задачи']
            result.append(finished_row)

    return result


def merge_region_and_supported_tasks(supported_tasks: pandas.DataFrame,
                                     tasks_from_region: list) -> list:
    filter = ('Макрорегион', set([_['Макрорегион'] for _ in tasks_from_region]))
    support_rows = df_to_rows(filter, supported_tasks)
    for support_row in support_rows:
        if support_row['Process ODn'] not in [_['Process ODn'] for _ in tasks_from_region]:
            tasks_from_region.append(support_row)

    for row in tasks_from_region[::]:
        if row['Process ODn'] not in [_['Process ODn'] for _ in support_rows]:
            index_for_delete = tasks_from_region.index(row)
            tasks_from_region.pop(index_for_delete)

    return tasks_from_region


def df_to_rows(filter: tuple = None, df: pandas.DataFrame = None):
    if filter:
        df = df[df.columns].loc[df[filter[0]].isin(filter[1])]
    df = df.rename(columns={'Имя': 'Тип задачи'})
    rows = []
    for index, row in df.iterrows():
        data_support_row = dict(zip(df.columns, row))
        rows.append(data_support_row)
    return rows


def fill_region_file_from_supported_and_finished(input_file: str,
                                                 output_file: str,
                                                 supported_tasks: pandas.DataFrame,
                                                 finished_tasks: pandas.DataFrame) -> pandas.DataFrame:
    print(f'Читаем все задачи из файла {input_file}')
    region_tasks = get_tasks_from_region(file=input_file)
    print(f'Объединяем завершенные задачи и задачи из файла {input_file}')

    merged_with_finished = merge_region_and_finished_tasks(
        finished_tasks=finished_tasks,
        tasks_from_region=region_tasks
    )
    print(f'Объединяем поддерживаемые задачи и задачи из файла {input_file}')
    merged_with_support = merge_region_and_supported_tasks(
        supported_tasks=supported_tasks,
        tasks_from_region=region_tasks
    )

    for _ in merged_with_finished:
        if _ not in merged_with_support:
            merged_with_support.append(_)
    print(f'Формируем итоговый файл {output_file}')
    result = pandas.DataFrame(merged_with_support)
    return result
