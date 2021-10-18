from module.functions import get_all_tasks_from_excel, \
    get_supported_tasks, \
    get_finished_tasks, \
    stylize_and_write, \
    fill_region_file_from_supported_and_finished

if __name__ == '__main__':
    RRL_file = 'E:\\Новая папка\\WFL_расширение_РРЛ_подробный_с_фильтрами.xlsx'
    region_files = [
        r'\\app.corp.tele2.ru@SSL\DavWWWRoot\Transport_planning\Documents\4пика\RRL_Integrity\БиДВ.xlsx',
        r'\\app.corp.tele2.ru@SSL\DavWWWRoot\Transport_planning\Documents\4пика\RRL_Integrity\Волга.xlsx',
        r'\\app.corp.tele2.ru@SSL\DavWWWRoot\Transport_planning\Documents\4пика\RRL_Integrity\Москва.xlsx',
        r'\\app.corp.tele2.ru@SSL\DavWWWRoot\Transport_planning\Documents\4пика\RRL_Integrity\Северо-запад.xlsx',
        r'\\app.corp.tele2.ru@SSL\DavWWWRoot\Transport_planning\Documents\4пика\RRL_Integrity\Сибирь.xlsx',
        r'\\app.corp.tele2.ru@SSL\DavWWWRoot\Transport_planning\Documents\4пика\RRL_Integrity\Урал.xlsx',
        r'\\app.corp.tele2.ru@SSL\DavWWWRoot\Transport_planning\Documents\4пика\RRL_Integrity\Центр.xlsx',
        r'\\app.corp.tele2.ru@SSL\DavWWWRoot\Transport_planning\Documents\4пика\RRL_Integrity\Черноземье.xlsx',
        r'\\app.corp.tele2.ru@SSL\DavWWWRoot\Transport_planning\Documents\4пика\RRL_Integrity\Юг.xlsx',
    ]
    finished_file = r'C:\Users\vladimir.tyron\Desktop\завершенные.xlsx'
    supported_file = r'C:\Users\vladimir.tyron\Desktop\поддерживаемые.xlsx'

    print(f'Читаем все задачи из файла {RRL_file}')
    all_tasks = get_all_tasks_from_excel(file=RRL_file)

    print('Получаем поддерживаемые...')
    supported_tasks = get_supported_tasks(base_dataframe=all_tasks)
    print(f'Записываем поддерживаемые в файл {supported_file}')
    stylize_and_write(
        dataframe=supported_tasks,
        filename=supported_file
    )

    print('Получаем завершенные...')
    finished_tasks = get_finished_tasks(base_dataframe=all_tasks)
    print(f'Записываем завершенные в файл {finished_file}')
    stylize_and_write(
        dataframe=finished_tasks,
        filename=finished_file
    )

    for file in region_files:
        output_file = file
        # output_file = file.replace('.xlsx', '_skr.xlsx')
        result = fill_region_file_from_supported_and_finished(
            input_file=file,
            output_file=output_file,
            supported_tasks=supported_tasks,
            finished_tasks=finished_tasks
        )
        print(f'Записываем файл {output_file}')
        stylize_and_write(
            dataframe=result,
            filename=output_file
        )
