# -*- coding:utf-8 -*-
__author__ = 'shijin'

import os
import sys
import getopt
import time
import re
import jieba
import difflib
from pprint import pprint
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from utils.util_logfile import nlogger, flogger, slogger, traceback
from utils.util_xlsx import HandleXLSX
from utils.util_re import re_bank, re_like_bank, re_agency_company, re_sale_company, re_appraisal_company, \
    re_economic_company, re_insurance, re_like_insurance, re_company
from datetime import datetime
from settings import TASK_WAITING_TIME, MAX_WORKERS, SAMPLES_FILE, EXCLUDE_WORDS
from row_object import RowStatus
from extract_name import extract_company_name
from structure_sample import get_samples_object, Samples


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
        _dict_file_name = check_file_name(SAMPLES_FILE, **kwargs)
        # _dict_file_name = check_file_name('会员单位名单.xlsx', **kwargs)
        _dict_xls, _dict_row_object_iterator = get_row_object_iterator(check_file, _dict_file_name, 'listing', **kwargs)
        samples_object = get_samples_object(_dict_row_object_iterator, **kwargs)
        nlogger.info(f"get_samples_object has been completed")

        # Prepare source data
        _data_file_name = check_file_name(file_name, **kwargs)
        _data_xls, _data_row_object_iterator = get_row_object_iterator(check_file, _data_file_name, sheet_name,
                                                                       start_point, end_point, **kwargs)
        nlogger.info(f"handle_data_thread start")
        _data_result = handle_data_thread(row_object_iterator=_data_row_object_iterator, samples_object=samples_object,
                                          **kwargs)
        nlogger.info(f"write_result_to_xls start")
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
    assert kwargs.get('samples_object') and isinstance(kwargs.get('samples_object'),
                                                       Samples), "Parameter samples_object must be instance of Samples"
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
        assert kwargs.get('company_name'), "company name is invalid"
        assert kwargs.get('samples_object'), "company dict is invalid"

        _row_object = set_row_object_company_info(row_object, **kwargs)

        return _row_object
    except AssertionError:
        raise
    except Exception as e:
        nlogger.error("{fn} error: {e}".format(fn='handle_data', e=traceback.format_exc()))
        raise HandleDataError("{fn} error: {e}".format(fn='handle_data', e=repr(e)))


def set_row_object_company_info(row_object, samples_object, company_name, **kwargs):
    """
    add row object property that include company name and company type
    :param row_object:
    :param samples_object: instance of class Samples
    :param company_name: extract company'name from '公司' column in source excel
    :param kwargs:
    :return:
    """

    # _company_info = samples_object.all_name.get(company_name)
    _company_info = samples_object.recursive_search_key_value('all_name', company_name)
    # print("###" * 30)
    # print(f"{row_object.position} company_name: {company_name}, _company_info: {_company_info}")
    # print("###" * 30)
    if _company_info:
        row_object.column_value['result'] = f'已知单位'
        row_object.column_value['company_name'] = company_name
        row_object.column_value['guess_name'] = ''
        row_object.column_value['company_full_name'] = _company_info.get('full_name')
        row_object.column_value['company_type'] = _company_info.get('company_type')
        row_object.column_value['similarity'] = ''
        row_object.status = RowStatus.EXISTENCE.value
    # elif len(company_name) > 14 and re_company(company_name):
    #     row_object.column_value['result'] = f'未匹配单位'
    #     row_object.column_value['company_name'] = company_name
    #     row_object.column_value['guess_name'] = ''
    #     row_object.column_value['company_full_name'] = company_name
    #     row_object.column_value['company_type'] = match_company_type(company_name)[0]
    #     row_object.column_value['similarity'] = ''
    #     row_object.status = RowStatus.NONEXISTENCE.value
    elif re_like_bank(company_name):
        row_object.column_value['result'] = f'银行系统'
        row_object.column_value['company_name'] = company_name
        row_object.column_value['guess_name'] = ''
        row_object.column_value['company_full_name'] = company_name
        row_object.column_value['company_type'] = match_company_type(company_name)[0]
        row_object.column_value['similarity'] = ''
        row_object.status = RowStatus.NONEXISTENCE.value
    else:
        similarity_result, similarity_company_name = similarity_match_company_name(samples_object, company_name,
                                                                                   **kwargs)
        if similarity_result <= 0.5:
            row_object.column_value['result'] = f'未知单位'
            row_object.column_value['company_name'] = company_name
            row_object.column_value['guess_name'] = ''
            row_object.column_value['company_full_name'] = company_name
            row_object.column_value['company_type'] = match_company_type(company_name)[0]
            row_object.column_value['similarity'] = ''
            row_object.status = RowStatus.NONEXISTENCE.value
        else:
            _company_info = samples_object.recursive_search_key_value('all_name', similarity_company_name)
            # print("***" * 30)
            # print(f"{row_object.position} company_name: {company_name}, _company_info: {_company_info}")
            # print("***" * 30)
            # _company_info = samples_object.recursive_search_key_value('all_name', company_name)
            # _company_info = company_dict.get(similarity_company_name)

            if similarity_result > 0.9:
                row_object.column_value['result'] = f'近似单位'

            elif similarity_result >= 0.8:
                row_object.column_value['result'] = f'相似单位'

            elif similarity_result > 0.5:
                row_object.column_value['result'] = f'猜测单位'

            row_object.column_value['company_name'] = company_name
            row_object.column_value['guess_name'] = _company_info.get('full_name')
            row_object.column_value['company_full_name'] = ''
            row_object.column_value['company_type'] = _company_info.get('company_type')
            row_object.column_value['similarity'] = round(similarity_result, 3)
            row_object.status = RowStatus.SIMILARITY.value

    return row_object


def get_company_name_search_type(company_name, **kwargs):
    """
    Match company type
    :param company_name:
    :param kwargs:
    :return:
    """
    _company_type, _samples_property_name = match_company_type(company_name)
    return _samples_property_name


def get_full_or_abbr_property(company_name, length=12, **kwargs):
    """
    Judge full or abbreviation
    :param company_name:
    :param length:
    :param kwargs:
    :return:
    """
    _full_or_abbr = "abbr" if len(company_name) < length else "full"
    return str(_full_or_abbr).strip()


def get_samples_property(samples_object, samples_property_name, full_or_abbr, **kwargs):
    """
    structure samples property name and get property(dict)
    :param samples_object:
    :param samples_property_name:
    :param full_or_abbr:
    :param kwargs:
    :return:
    """
    _property_name = f"{samples_property_name}_{full_or_abbr}_name"
    return samples_object.get_property(_property_name)


def similarity_match_company_name(samples_object, company_name, **kwargs):
    similarity_result = 0
    similarity_company_name = None
    _company_name = str(company_name).strip()

    _samples_property_name = get_company_name_search_type(_company_name, **kwargs)
    _full_or_abbr = get_full_or_abbr_property(_company_name, **kwargs)
    _samples_company_name_dict = get_samples_property(samples_object, _samples_property_name, _full_or_abbr, **kwargs)

    # _company_name = filter_company_name(company_name)   ?????

    if _full_or_abbr == 'full':
        _company_name_list = jieba.lcut(_company_name)
        for samples_company_name in _samples_company_name_dict.keys():
            _samples_company_name_list = jieba.lcut(samples_company_name)
            similarity_rate = difflib.SequenceMatcher(None, _company_name_list, _samples_company_name_list).ratio()
            if similarity_rate > similarity_result:
                similarity_result = similarity_rate
                similarity_company_name = samples_company_name
    else:
        for samples_company_name in _samples_company_name_dict.keys():
            similarity_rate = difflib.SequenceMatcher(None, _company_name, samples_company_name).ratio()
            if similarity_rate > similarity_result:
                similarity_result = similarity_rate
                similarity_company_name = samples_company_name

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
    :return: company type, Samples property name
    """
    if re_like_bank(company_name):
        _company_type = "银行"
        _samples_property_name = 'bank'
    elif re_appraisal_company(company_name):
        _company_type = "公估公司"
        _samples_property_name = 'insurance_appraisal'
    elif re_economic_company(company_name):
        _company_type = "经纪公司"
        _samples_property_name = 'insurance_economic'
    elif re_agency_company(company_name):
        _company_type = "代理公司"
        _samples_property_name = 'insurance_agency'
    elif re_sale_company(company_name):
        _company_type = "代理公司"
        _samples_property_name = 'insurance_sale'
    elif re_like_insurance(company_name):
        _company_type = "保险公司"
        _samples_property_name = 'insurance_company'
    else:
        _company_type = "相关机构"
        _samples_property_name = 'all'  # 如果上述判定不了，就全部比较匹配

    return str(_company_type).strip(), str(_samples_property_name).strip()


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

        y_guess_name = recursive_get_index(column_name_list, 'guess_name') + 1
        if y_guess_name > columns_number:
            source_xls.write_sheet_rows_value(sheet_name=source_xls.sheet.title, values=(1, y_guess_name, 'guess_name'))

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

            if row_object.column_value.get('guess_name'):
                y = y_guess_name
                values = (x, y, row_object.column_value.get('guess_name'))
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
