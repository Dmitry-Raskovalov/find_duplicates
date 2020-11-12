"""
Скрипт сравнивает файлы в 2-х директориях, включая поддиректории,
и ищет дубликаты. Структура каталогов не имеет значения.
Формируется текстовый файл отчет по которому производится перенос файлов.
Отчет можно отредактировать если какие то файлы удалять не нужно.
Дубликаты удаляются из slave directory.
По факту файлы не удаляются, а переносятся в директорию 'recycle_duplicates'
которая создается в родителе SLAVE_DIRECTORY. Эту папку нужно удалить
средствами ОС.
Интерфейса нет. Функционал реализуется запуском функций dupl_report,  run_replace_duplicates.
Поиск уникальных файлов в разработке.
"""

import filecmp
import os

# директория с которой будет сравниваться
MASTER_DIRECTORY = 'd:\\temp\\test_dirs\\m_dir'
# директория из которой будут удалены дубликаты
SLAVE_DIRECTORY = 'd:\\temp\\test_dirs\\s_dir'
# директории исключенные из проверки
EXCEPTION_DIR = ()
# путь к отчету
REPORT_DIR = 'd:\\temp\\test_dirs'
# название файла с отчетом (без расширения)
REPORT_NAME = 'unique_report'
# тип отчета. 2 варианта 'csv' или 'txt'
# csv удобно открыть в редакторе таблиц и сделать сортировку по папкам
# txt можно использовать при небольшом количестве файлов
REPORT_TYPE = 'csv'
# Разделитель в csv файле (обычно запятая или точка с запятой)
CSV_SEPARATOR = ';'
REPORT_FILE = REPORT_NAME + '.' + REPORT_TYPE


def get_same_size_file(path, except_dirs, file_list={}, same_size_files=[]):
    # file_list = {размер_файла: ['путь к файлу', ]}
    # same_size_files = [размер файлов]

    # file_list = {} if not file_list else file_list
    # same_size_files = [] if not same_size_files else same_size_files
    except_dirs = [path.strip() for path in except_dirs]
    for dirs, subdirs, files in os.walk(path):
        if dirs not in except_dirs:
            for file in files:
                file_path = os.path.join(dirs, file)
                file_size = os.path.getsize(file_path)
                if file_size in file_list:
                    file_list[file_size].append(file_path)
                    if len(file_list[file_size]) == 2:
                        same_size_files.append(file_size)
                else:
                    file_list[file_size] = [file_path]
    print('проиндексировано {} файлов \n совпадений размера {}'
          .format(len(file_list), len(same_size_files)))
    return file_list, same_size_files


def compare_files(current_file, file_list):
    dupl, dif = [], []
    for file in file_list:
        try:
            if filecmp.cmp(current_file, file):
                dupl.append(file)
            else:
                dif.append(file)
        except OSError as e:
            print('файлы {}\n{}\n вызвали исключение {}'.format(
                current_file, file, e))
    return dupl, dif


def get_duplicates_list(size_file_list, same_size_files):
    # сравнивает по содержанию файлы одинаковые по размеру
    # возващает список списков с идентичными файлами
    duplcates= []
    for size in same_size_files:
        file_list = size_file_list[size]
        while len(file_list) > 1:
            current_file = file_list.pop(0)
            dupl, dif = compare_files(current_file, file_list)
            if dupl:
                dupl.append(current_file)
                duplcates.append(dupl)
    return duplcates

def compare_master_slave(master_dir=MASTER_DIRECTORY, slave_file_list):
    # создать словарь мастер директории
    # цикл перебора мастер директории и сравнения со слэйв по размеру и имени файла
    # в случае нескольких файлов в слэйв полное сравнение с каждым
    # при совпадеении - удаление из слэйв словаря
    # что останется в слэйв - уникальные файлы
    pass

def get_unique_file_list(size_file_list, same_size_files):
    """
    возвращает словарь в котором только уникальные файлы
    из дублирующихся файлов оставлен только один
    """
    unique_files_dict = size_file_list.copy()
    for size in same_size_files:
        file_list = size_file_list[size]
        uniques = []
        while len(file_list) > 1:
            current_file = file_list.pop(0)
            dupl, dif = compare_files(current_file, file_list)
            uniques.append(current_file)
            file_list = dif.copy()
        else:
            uniques.append(file_list[0])
        unique_files_dict[size] = uniques
    return unique_files_dict


def make_report(duplicates, master_dir, rep_type=REPORT_TYPE):
    """
    Созает файл отчета по которому дубликаты будут удаляться.
    Удаляются дубликаты из slave папки
    Предполагается что файл отчета нужно отредактировать руками
    в TXT отчете
    если какие то файлы удалять не нужно необходимо перед ним в начале строки
    поставить знак табуляции или любой символ,
    но НЕ пробелы! пробелы в начале и в конце игнорируются.
    в CSV отчете удаляются файлы из первой колонки
    """
    lines_for_report = lines_for_csv if rep_type == 'csv' else lines_for_txt
    with open(report_path(), 'w+') as report_file:
        for dupl in duplicates:
            for line in lines_for_report(dupl, master_dir):
                report_file.write(line)
    return report_path()


def report_path(report_name=REPORT_FILE, report_dir=REPORT_DIR):
    report_dir = os.getcwd() if not report_dir else report_dir
    return os.path.join(report_dir, report_name)


def make_unique_file_report(unique_files, slave_dir=SLAVE_DIRECTORY,
                            master_dir=MASTER_DIRECTORY, sep=CSV_SEPARATOR):
    with open(report_path(), 'w+') as report_file:
        copy_path = master_dir + '\\!!new_files'
        for path in unique_files:
            rp = os.path.relpath(path, start=slave_dir)
            rp = os.path.join(copy_path, rp)
            line = path + sep + rp + '\n'
            report_file.write(line)


def lines_for_csv(duplicates, master_dir, sep=CSV_SEPARATOR):
    #  *** ставится перед путем во второй колонке
    #  если найдено несколько дублей файла в slave dir
    #  что бы обозначить что это дубли одного файла
    master, slave = [], []
    for file_path in duplicates:
        if file_path.startswith(master_dir):
            master.append(file_path)
        else:
            slave.append(file_path)
    if not len(master):
        master.append(slave.pop(-1))
    report_lines = []
    n = len(master) if len(master) > len(slave) else len(slave)
    for i in range(n):
        m, prefix_m = (master[i], '') if i < len(master) else (m, '***')
        if not len(slave):
            s = '***'
            prefix_s = ''
        else:
            s, prefix_s = (slave[i], '') if i < len(slave) else (s, "***")
        line = prefix_s + s + sep + os.path.split(s)[0] + sep + \
            prefix_m + m + sep + os.path.split(m)[0] + '\n'
        report_lines.append(line)
    return report_lines


def lines_for_txt(duplicates, master_dir):
    # В TXT Перед файлами которые НЕ удаляются в отчете ставтся знак табуляции

    not_in_master_dir = True
    report_lines = []
    for file_path in duplicates:
        if file_path.startswith(master_dir):
            prefix = '\t'
            not_in_master_dir = False
        else:
            prefix = ''
        path = prefix + file_path + '\n'
        report_lines.append(path)
    if not_in_master_dir:
        # если в списке дубликатов нет ни одного из мастер директории
        # нужно оставить один файл и не удалять его
        report_lines[-1] = '\t' + report_lines[-1]
    return report_lines


def replace_duplicates(report_file, slave_dir, sep=CSV_SEPARATOR):
    with open(report_file, 'r') as report:
        recycle_path = os.path.join(os.path.dirname(slave_dir),
                                    'recycle_duplicates')
        n = 0
        if not os.path.exists(recycle_path):
            os.mkdir(recycle_path)
        for line in report:
            duplicate_path = line.split(sep=sep)[0]
            duplicate_path.rstrip('\n')
            if os.path.exists(duplicate_path):
                rp = os.path.relpath(duplicate_path, start=slave_dir)
                rp = os.path.join(recycle_path, rp)
                os.renames(duplicate_path, rp)
                n += 1
        print('Дубликаты файлов были перенесены в директорию', recycle_path,
              '\nУбедитесь что нужные файлы не были перенесены,',
              ' и удалите эту директорию средствами ОС',
              '\nБыло перенесено {} файлов'.format(n))


def find_duplicates_and_unique_files(mode):
    print('поиск запущен')
    file_list, same_size_files = get_same_size_file(MASTER_DIRECTORY,
                                                    EXCEPTION_DIR)
    file_list, same_size_files = get_same_size_file(
        SLAVE_DIRECTORY,
        EXCEPTION_DIR,
        file_list=file_list,
        same_size_files=same_size_files
    )
    duplicates = get_duplicates_list(file_list, same_size_files)
    if mode == 'unique':
        return get_unique_file_list(file_list, same_size_files)
    elif mode == 'dupl':
        return duplicates


def uniques_report():
    """ поиск уникальных файлов и создание отчета """ 
    make_unique_file_report(find_duplicates_and_unique_files('unique'))


def dupl_report():
    """ поиск дублей и создание отчета"""
    make_report(find_duplicates_and_unique_files('dupl'), MASTER_DIRECTORY)


def run_replace_duplicates():
    """ перенос файлов из отчета в директорию для удаления"""
    replace_duplicates(report_path(), SLAVE_DIRECTORY)


uniques_report()
