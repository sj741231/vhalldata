import os
import sys
import getopt
import time
import re
import difflib
import jieba
from utils.util_xlsx import HandleXLSX
from utils.util_re import re_remove_sub_company
from settings import PROVINCE


def extract_company_name(company_name):
    # company_name = str(company_name).strip()
    _company_name = "".join(str(company_name).split())
    pre_company_name = pre_trim_company_name(_company_name)
    mid_company_name = mid_trim_company_name(pre_company_name)
    return mid_company_name


def pre_trim_company_name(company_name):
    if company_name.find('公司') != -1:
        pre_company_name = trim_string(company_name, '公司')
    elif company_name.find('银行') != -1:
        pre_company_name = trim_string(company_name, '银行')
    else:
        pre_company_name = company_name

    # pre_company_name = re.split('省|市|县|区', pre_company_name)[0]
    # _re_compile = re.compile(r'^.{4,}(支|分)+')
    # if _re_compile.match(pre_company_name):
    #     pre_company_name = re.sub(r'(省|市|县|区)', '', pre_company_name)
    return pre_company_name


def mid_trim_company_name(company_name):
    """
    Remove sub company name that include '支' or '分'
    Check keyword in Province by using jieba
    Recursive get adjacent remove_words that consist of keywords
    :param company_name:
    :return:
    """
    # _re_compile = re.compile(r'^.{4,}(支|分)+')
    # if _re_compile.match(company_name):
    #     mid_company_name = re.sub(r'(省|市|县|区)', '', company_name)

    if re_remove_sub_company(company_name):
        mid_company_name = re.sub(r'(省|市|县|区)', '', company_name)

        company_name_list = list(jieba.lcut(mid_company_name))
        company_name_list_len = len(company_name_list)

        if company_name_list_len > 1:
            company_name_list.reverse()
            for idx in range(0, company_name_list_len - 1):
                if company_name_list[idx] in PROVINCE:
                    remove_words = recursive_get_keyword(company_name_list, company_name_list_len, idx)
                    mid_company_name = trim_string(mid_company_name, remove_words, start=2, reserve=False)
                    if re_remove_sub_company(mid_company_name):
                        continue
                    else:
                        return mid_company_name
            return mid_company_name
        else:
            return mid_company_name
    else:
        return company_name


def recursive_get_keyword(company_name_list, company_name_list_len, idx, keyword=None):
    """
    Recursive search for adjacent keywords that need to be removed
    :param company_name_list:
    :param company_name_list_len:
    :param idx:
    :param keyword:
    :return:
    """
    if keyword is None:
        keyword = company_name_list[idx]
    else:
        keyword = keyword + company_name_list[idx]

    idx += 1
    if idx >= (company_name_list_len - 1) or company_name_list[idx] not in PROVINCE:
        return keyword
    else:
        return recursive_get_keyword(company_name_list, company_name_list_len, idx, keyword)


def trim_string(full_string, sub_string, start=None, end=None, reserve=True, trim_sort=True):
    _full_string = str(full_string).strip()
    _sub_string = str(sub_string).strip()
    if _full_string and _sub_string:
        _sub_string_length = len(_sub_string)

        if trim_sort is True:
            idx = _full_string.find(sub_string, start, end)
            # print("*** ", _full_string, _sub_string, _sub_string_length, idx)
            if idx != -1:
                return _full_string[:idx + _sub_string_length] if reserve is True else _full_string[:idx]
            else:
                return _full_string
        else:
            idx = _full_string.rfind(sub_string, start, end)
            if idx != -1:
                return _full_string[:idx + _sub_string_length] if reserve is True else _full_string[:idx]
            else:
                return _full_string
    else:
        return _full_string


if __name__ == "__main__":
    print("###" * 30)

    filename = '报名表单数据.xlsx'
    xls = HandleXLSX(filename)
    # rows = xls.generator_rows_value_iterator(sheet_name='listing', start_point=1, end_point=10)
    #
    # for i in rows:
    #     print("row: ", i)

    print("***" * 30)
    rows = xls.generate_row_object_iterator(check_file=False, sheet_name='listing', start_point=2, end_point=10000)

    # for j in rows:
    #     print("row: ", j, j.__dict__)

    for i in rows:
        print(i.position, i.column_value.get('公司'), extract_company_name(i.column_value.get('公司')))
