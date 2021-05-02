# -*- coding:utf-8 -*-
__author__ = 'shijin'

import os
import sys
import getopt
import time
import re
import difflib
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from utils.util_logfile import nlogger, flogger, slogger, traceback
from utils.util_xlsx import HandleXLSX
from utils.util_re import re_bank, re_agency_company, re_appraisal_company, re_economic_company
from datetime import datetime
from settings import TASK_WAITING_TIME, MAX_WORKERS, DICT_FILE, EXCLUDE_WORDS
from row_object import RowStatus
from extract_name import extract_company_name


class GetURLError(Exception):
    pass


class GetRowIterError(Exception):
    pass


class HandleDataError(Exception):
    pass


class ThreadTaskError(Exception):
    pass


class WriteResultError(Exception):
    pass


def exec_func(check_file, file_name=None, sheet_name=None, start_point=None, end_point=None, **kwargs):
    """
    Executive Function
    :param check_file: if check_file is True,then only check if download file exists. default False
    :param file_name: Excel file name
    :param sheet_name: sheet name, default active sheet
    :param start_point: start row number, minimum is 2 ( row 1 is column name)
    :param end_point: end row number , maximum is the row number of sheet
    :param kwargs:
    :return:
    """
    try:
        # Construct dictionary of company
        _dict_file_name = check_file_name(DICT_FILE, **kwargs)
        # _dict_file_name = check_file_name('会员单位名单.xlsx', **kwargs)
        _dict_xls, _dict_row_object_iterator = get_row_object_iterator(check_file, _dict_file_name, 'dict', **kwargs)
        company_dict = get_company_dict(_dict_row_object_iterator, **kwargs)

        # Prepare source data
        _data_file_name = check_file_name(file_name, **kwargs)
        _data_xls, _data_row_object_iterator = get_row_object_iterator(check_file, _data_file_name, sheet_name,
                                                                       start_point, end_point, **kwargs)

        _data_result = handle_data_thread(row_object_iterator=_data_row_object_iterator, company_dict=company_dict,
                                          **kwargs)
        write_result_to_xls(_data_xls, _data_result)
    except (GetRowIterError, HandleDataError, ThreadTaskError, WriteResultError) as e:
        nlogger.error('{fn} Custom error: {e}'.format(fn='exec_func', e=repr(e)))
        print(f'Custom error: {repr(e)}')
    except AssertionError as e:
        nlogger.error('{fn} Assertion error: {e}'.format(fn='exec_func', e=traceback.format_exc()))
        print(repr(e))
    except Exception as e:
        nlogger.error('{fn} error: {e}'.format(fn='exec_func', e=traceback.format_exc()))
        print(f'Undefined error: {repr(e)}')


def check_file_name(file_name, **kwargs):
    """
    check if the file exists
    :param file_name:
    :param kwargs:
    :return:
    """
    assert file_name is not None, "Parameter file_name must be provided and is not None."
    _file_name = str(file_name).strip()
    assert os.path.exists(_file_name), "file_name {f} does not exists".format(f=_file_name)
    return _file_name


def get_company_dict(row_object_iterator, **kwargs):
    company_dict = dict()
    for row_object in row_object_iterator:
        abbr_name = row_object.column_value.get('简称')
        full_name = row_object.column_value.get('单位名称')
        if full_name:
            company_dict[full_name] = {'full_name': row_object.column_value.get('单位名称'),
                                       'company_type': row_object.column_value.get('单位类别')}
        if abbr_name:
            company_dict[abbr_name] = {'full_name': row_object.column_value.get('单位名称'),
                                       'company_type': row_object.column_value.get('单位类别')}
            if re_economic_company(abbr_name):
                idx = abbr_name.find('经纪')
                company_dict[abbr_name[:idx]] = {'full_name': row_object.column_value.get('单位名称'),
                                                 'company_type': row_object.column_value.get('单位类别')}
            elif re_agency_company(abbr_name):
                idx = abbr_name.find('代理') if abbr_name.find('代理') else abbr_name.find('销售')
                company_dict[abbr_name[:idx]] = {'full_name': row_object.column_value.get('单位名称'),
                                                 'company_type': row_object.column_value.get('单位类别')}
            elif re_appraisal_company(abbr_name):
                idx = abbr_name.find('公估')
                company_dict[abbr_name[:idx]] = {'full_name': row_object.column_value.get('单位名称'),
                                                 'company_type': row_object.column_value.get('单位类别')}
    return company_dict


def get_row_object_iterator(check_file, file_name, sheet_name=None, start_point=None, end_point=None, **kwargs):
    """
    get iterator of row object
    :param check_file: True or False
    :param file_name:
    :param sheet_name:
    :param start_point:
    :param end_point:
    :param kwargs:
    :return: instance of HandleXLSX object, iterator of row object
    """
    try:
        source_xls = HandleXLSX(file_name, sheet_name)
        row_object_iterator = source_xls.generate_row_object_iterator(check_file, sheet_name, start_point, end_point,
                                                                      **kwargs)
        return source_xls, row_object_iterator
    except Exception as e:
        nlogger.error('{fn} error: {e}'.format(fn='get_row_object_iterator', e=traceback.format_exc()))
        raise GetRowIterError('{fn} error: {e}'.format(fn='get_row_object_iterator', e=repr(e)))


# def handle_data(data_row_object_iterator, company_dict, **kwargs):
#     return _data_result


def handle_data_thread(row_object_iterator, **kwargs):
    """
    handle data by multi Thread
    :param row_object_iterator:
    :param kwargs:
    :return: The result of download, it's list
    """
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    try:
        data_result = []
        all_task = [executor.submit(handle_data_task, row_object, **kwargs) for row_object in row_object_iterator]

        # Raise TimeoutError: If the entire result iterator could not be generated before the given timeout.
        for future in as_completed(all_task, timeout=TASK_WAITING_TIME):
            data = future.result()
            if data:
                data_result.append(data)
        nlogger.info(f'Handle data completed, {len(data_result)} rows')
        return data_result
    except TimeoutError as e:
        nlogger.error("{fn} TimeoutError: {e}".format(fn='handle_data_thread', e=repr(e)))
        executor.shutdown(wait=True)  # 等待future 任务都执行完成后再关闭资源
        raise ThreadTaskError('{fn} TimeoutError: {e}'.format(fn='handle_data_thread', e=repr(e)))
    except Exception as e:
        nlogger.error("{fn} error: {e}".format(fn='handle_data_thread', e=traceback.format_exc()))
        flogger.error("{fn} error: {e}".format(fn='handle_data_thread', e=repr(e)))
        raise ThreadTaskError('{fn} error: {e}'.format(fn='handle_data_thread', e=repr(e)))


def handle_data_task(row_object, **kwargs):
    """
    handle task
    :param row_object: row object
    :param kwargs:
    :return:  The Row object that processed by task
    """
    try:
        check_row_object(row_object, **kwargs)
        _row_object = handle_data(row_object, **kwargs)
        return _row_object
    except AssertionError as e:
        row_object.status = RowStatus.ERROR.value
        nlogger.error("{fn} Params error: {e}".format(fn='handle_data_task', e=traceback.format_exc()))

        if hasattr(row_object, 'column_value') and isinstance(row_object.column_value, dict):
            row_object.column_value['result'] = f'Params AssertionError: {str(e)}'
            _company_name = str(row_object.column_value.get('公司', '未知单位')).strip()
        else:
            setattr(row_object, 'column_value', {'result': f'Params AssertionError: {str(e)}'})
            _company_name = '未知单位'

        flogger.error("handle_data_task failed:{n},AssertionError:{e}".format(n=_company_name, e=repr(e)))
        return row_object
    except HandleDataError as e:
        row_object.status = RowStatus.ERROR.value
        _company_name = str(row_object.column_value.get('公司', '未知单位')).strip()
        flogger.error("handle_data_task failed:{n},HandleDataError:{e}".format(n=_company_name, e=repr(e)))
        row_object.column_value['result'] = f'HandleDataError: {str(e)}'
        return row_object
    except Exception as e:
        row_object.status = RowStatus.ERROR.value
        nlogger.error("{fn} position:{p}, company name:{n}, undefined error: {e}".format(fn='handle_data_task',
                                                                                         p=row_object.position,
                                                                                         n=row_object.column_value[
                                                                                             '公司'],
                                                                                         e=traceback.format_exc()))
        row_object.column_value['result'] = f'handle_data_task undefined error: {str(e)}'
        _company_name = str(row_object.column_value.get('公司', '未知单位')).strip()
        flogger.error("handle_data_task undefined failed:{n},HandleDataError:{e}".format(n=_company_name, e=repr(e)))
        return row_object


def check_row_object(row_object, **kwargs):
    """
    check Row object property
    :param row_object:
    :param kwargs:
    :return:
    """
    assert kwargs.get('company_dict') and isinstance(kwargs.get('company_dict'),
                                                     dict), "Parameter company_dict must be dict and not be empty"
    assert isinstance(row_object.file_name, str) and str(
        row_object.file_name).strip(), "Invalid parameter file_name: {p}".format(p=str(row_object.file_name).strip())
    assert isinstance(row_object.sheet_name, str) and str(
        row_object.sheet_name).strip(), "Invalid parameter sheet_name: {p}".format(p=str(row_object.sheet_name).strip())
    assert row_object.column_name and isinstance(row_object.column_name,
                                                 (list, tuple)), "Parameter column_name is list and not be empty"
    assert row_object.row_value and isinstance(row_object.row_value,
                                               (list, tuple)), "Parameter row_value is list and not be empty"
    assert row_object.column_value and isinstance(row_object.column_value,
                                                  dict), "Parameter column_value is dict and not be empty"
    assert row_object.position and isinstance(row_object.position,
                                              int), "Parameter position is int and not be less than 1"
    assert row_object.status and RowStatus(row_object.status).name in ['INITIAL',
                                                                       'CHECK'], "Parameter status is invalid"


def handle_data(row_object, **kwargs):
    """
    Get absolute patch of file storage
    :param row_object:
    :param kwargs:
    :return:
    """
    try:
        # kwargs['company_name'] = str(row_object.column_value.get('公司')).strip()
        _source_company_name = str(row_object.column_value.get('公司')).strip()
        kwargs['company_name'] = extract_company_name(_source_company_name)
        assert kwargs.get('company_dict'), "company dict is invalid"
        assert kwargs.get('company_name'), "company name is invalid"

        _row_object = set_row_object_company_info(row_object, **kwargs)

        return _row_object
    except AssertionError:
        raise
    except Exception as e:
        nlogger.error("{fn} error: {e}".format(fn='handle_data', e=traceback.format_exc()))
        raise HandleDataError("{fn} error: {e}".format(fn='handle_data', e=repr(e)))


def set_row_object_company_info(row_object, company_dict, company_name, **kwargs):
    """
    add row object property that include company name and company type
    :param row_object:
    :param company_dict:
    :param company_name:
    :param kwargs:
    :return:
    """

    _company_info = company_dict.get(company_name)
    if _company_info:
        row_object.column_value['result'] = f'已知单位'
        row_object.column_value['company_name'] = company_name
        row_object.column_value['company_full_name'] = _company_info.get('full_name')
        row_object.column_value['company_type'] = _company_info.get('company_type')
        row_object.column_value['similarity'] = 1
        row_object.status = RowStatus.EXISTENCE.value
    elif re_bank(company_name):
        row_object.column_value['result'] = f'未知单位'
        row_object.column_value['company_name'] = company_name
        row_object.column_value['company_full_name'] = ''
        row_object.column_value['company_type'] = match_company_type(company_name)
        row_object.column_value['similarity'] = ''
        row_object.status = RowStatus.NONEXISTENCE.value
    else:
        similarity_result, similarity_company_name = similarity_match_company_name(company_dict, company_name, **kwargs)
        if similarity_result < 0.2:
            row_object.column_value['result'] = f'未知单位'
            row_object.column_value['company_name'] = company_name
            row_object.column_value['company_full_name'] = ''
            row_object.column_value['company_type'] = match_company_type(company_name)
            row_object.column_value['similarity'] = ''
            row_object.status = RowStatus.NONEXISTENCE.value
        else:
            _company_info = company_dict.get(similarity_company_name)

            if similarity_result > 0.8:
                row_object.column_value['result'] = f'近似单位'

            elif similarity_result > 0.5:
                row_object.column_value['result'] = f'相似单位'

            elif similarity_result >= 0.2:
                row_object.column_value['result'] = f'疑似单位'

            row_object.column_value['company_name'] = company_name
            row_object.column_value['company_full_name'] = _company_info.get('full_name')
            row_object.column_value['company_type'] = _company_info.get('company_type')
            row_object.column_value['similarity'] = round(similarity_result, 2)
            row_object.status = RowStatus.SIMILARITY.value

    return row_object


def similarity_match_company_name(company_dict, company_name, **kwargs):
    similarity_result = 0
    similarity_company_name = None
    _company_name = filter_company_name(company_name)

    for company_dict_name in company_dict.keys():
        similarity_rate = difflib.SequenceMatcher(None, company_dict_name, _company_name).ratio()
        if similarity_rate > similarity_result:
            similarity_result = similarity_rate
            similarity_company_name = company_dict_name
    # print("***" * 30)
    # print("_company_name ", _company_name)
    # print("similarity_company_name ", similarity_company_name)
    # print("similarity_result ", similarity_result)
    # print("***" * 30)
    return similarity_result, similarity_company_name


def trim_company_name(company_name):
    _company_name = re.split('省|市|自治区', company_name)[0]
    return _company_name


def filter_company_name(company_name):
    """
    Remove redundant word
    :param company_name:
    :return:
    """
    for key_word in EXCLUDE_WORDS:
        company_name = company_name.replace(key_word, '')
    return company_name


def match_company_type(company_name, **kwargs):
    """
    match company type
    :param company_name: company name
    :param kwargs:
    :return: company type
    """
    if re_bank(company_name):
        _company_type = "银行"
    elif re_economic_company(company_name):
        _company_type = "经纪公司"
    elif re_appraisal_company(company_name):
        _company_type = "公估公司"
    elif re_agency_company(company_name):
        _company_type = "代理公司"
    else:
        _company_type = "相关机构"
    return _company_type


def recursive_get_index(query_list, query_value):
    """
    Recursive get the index of value in list
    :param query_list:
    :param query_value:
    :return: index
    """
    try:
        return query_list.index(query_value)
    except ValueError:
        query_list.append(query_value)
        return recursive_get_index(query_list, query_value)


def write_result_to_xls(source_xls, data_result):
    """
    Write result in sheet of Excel
    :param source_xls:
    :param data_result:
    :return:
    """
    try:
        column_name_list = source_xls.get_column_name_list()
        columns_number = len(column_name_list)

        y_result = recursive_get_index(column_name_list, 'result') + 1
        if y_result > columns_number:
            source_xls.write_sheet_rows_value(sheet_name=source_xls.sheet.title, values=(1, y_result, 'result'))

        y_name = recursive_get_index(column_name_list, 'name') + 1
        if y_name > columns_number:
            source_xls.write_sheet_rows_value(sheet_name=source_xls.sheet.title, values=(1, y_name, 'name'))

        y_full_name = recursive_get_index(column_name_list, 'full_name') + 1
        if y_full_name > columns_number:
            source_xls.write_sheet_rows_value(sheet_name=source_xls.sheet.title, values=(1, y_full_name, 'full_name'))

        y_type = recursive_get_index(column_name_list, 'type') + 1
        if y_type > columns_number:
            source_xls.write_sheet_rows_value(sheet_name=source_xls.sheet.title, values=(1, y_type, 'type'))

        y_similarity = recursive_get_index(column_name_list, 'similarity') + 1
        if y_similarity > columns_number:
            source_xls.write_sheet_rows_value(sheet_name=source_xls.sheet.title, values=(1, y_similarity, 'similarity'))

        for row_object in data_result:
            x = row_object.position
            y = y_result
            values = (x, y, row_object.column_value.get('result', 'unknown'))
            source_xls.write_sheet_rows_value(sheet_name=row_object.sheet_name, values=values)

            if row_object.column_value.get('company_name'):
                y = y_name
                values = (x, y, row_object.column_value.get('company_name'))
                source_xls.write_sheet_rows_value(sheet_name=row_object.sheet_name, values=values)

            if row_object.column_value.get('company_full_name'):
                y = y_full_name
                values = (x, y, row_object.column_value.get('company_full_name'))
                source_xls.write_sheet_rows_value(sheet_name=row_object.sheet_name, values=values)

            if row_object.column_value.get('company_type'):
                y = y_type
                values = (x, y, row_object.column_value.get('company_type'))
                source_xls.write_sheet_rows_value(sheet_name=row_object.sheet_name, values=values)

            if row_object.column_value.get('similarity'):
                y = y_similarity
                values = (x, y, row_object.column_value.get('similarity'))
                source_xls.write_sheet_rows_value(sheet_name=row_object.sheet_name, values=values)

        _result_file_name = "result_{d}.xlsx".format(d=datetime.now().strftime('%Y%m%d-%H:%M:%S'))
        source_xls.save(_result_file_name)
        nlogger.info(f'Write result completed, output file: {_result_file_name}')
    except Exception as e:
        nlogger.error("{fn} error: {e}".format(fn='write_result_to_xls', e=traceback.format_exc()))
        raise WriteResultError("{fn} error: {e}".format(fn='write_result_to_xls', e=repr(e)))


def usage():
    """
    Command help
    :return:
    """
    info = \
        """
Usage:
    python3.9 handle.py -file [ -sheet -start -end ]
    Help:
     -h --help
     -c --check   <check whether download file exists and do nothing>   
     
    Mandatory options:
     -f --file    <source excel>
    
    Optional options:
     -t --sheet   <The sheet name in Excel, default active sheet in Excel>
     -s --start   <Excel start row. Must be int,default index 2>
     -e --end     <Excel end row. Must be int,default Maximum number of rows in Excel>
        """
    print(info)


def main(argv):
    """
    Entrance function
    :param argv: command parameters
    :return:
    """
    _check_file = False
    _file_name = None
    _sheet_name = None
    _start_point = None
    _end_point = None

    try:
        opts, args = getopt.getopt(argv, "hcf:t:s:e:", ["help", "file", "sheet", "start", "end"])  # 短选项和长选型模式
    except getopt.GetoptError:
        print("Usage 1: python3.9 handle.py -h -c -f <source excel>  -t <excel sheet>  -s <start row index>  "
              "-e <end row index> \nUsage 2: python3.9 handle.py --help --check --file <source excel>  "
              "--sheet <excel sheet> --start <start row index>  --end <end row index>")
        sys.exit(2)  # 2 Incorrect Usage

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage()
            sys.exit(0)
        elif opt in ('-c', '--check'):
            _check_file = True
        elif opt in ('-f', '--file'):
            _file_name = str(arg).strip()
        elif opt in ('-t', '--sheet'):
            _sheet_name = str(arg).strip()
        elif opt in ('-s', '--start'):
            _start_point = str(arg).strip()
        elif opt in ('-e', '--end'):
            _end_point = str(arg).strip()

    if _file_name is None:
        print("Invalid parameter, -f --file must be provided. \nTry '-h --help' for more information.")
        sys.exit(2)

    if _start_point is not None and not _start_point.isdigit():
        print("Invalid parameter, -s --start must be followed by integer. \nTry '-h --help' for more information.")
        sys.exit(2)

    if _end_point is not None and not _end_point.isdigit():
        print("Invalid parameter, -e --end must be followed by integer. \nTry '-h --help' for more information.")
        sys.exit(2)

    params = dict(check_file=_check_file, file_name=_file_name, sheet_name=_sheet_name, start_point=_start_point,
                  end_point=_end_point)

    exec_func(**params)


if __name__ == "__main__":
    # file_name = EXCEL_FILE
    # sheet_name = 'listing'
    # start_point = 2
    # end_point = 3
    # kwargs = dict()
    # exec_func(file_name, sheet_name=None, start_point=None, end_point=None, **kwargs)
    main(sys.argv[1:])

    # python3.9 handle.py --file '报名表单数据.xlsx' --sheet 'listing' --start 2 --end 10000
    # python3.9 handle.py -f 'vid-20210214.xlsx' -t 'listing' -s 2 -e 4

    # print("***" * 30)
    # print(f'pre_row_object: {pre_row_object}', type(pre_row_object))
    # print(f'root_path: {root_path}', type(root_path))
    # print(f'force_download: {force_download}', type(force_download))
    # print("***" * 30)