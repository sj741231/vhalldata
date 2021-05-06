import os
import sys
import re
import jieba
from utils.util_xlsx import HandleXLSX
from utils.util_re import re_remove_sub_company, re_full_company_name
from settings import PROVINCE, EXCLUDE_WORDS, EXTRACT_WORDS


def extract_company_name(company_name):
    _company_name = str(company_name).strip()
    _company_name = _company_name.replace(" ", "")

    pre_company_name = pre_trim_company_name(_company_name)
    mid_company_name = mid_trim_company_name(pre_company_name)
    post_company_name = post_trim_company_name(mid_company_name)

    return post_company_name


def post_trim_company_name(company_name):
    """
    If company name include word in EXTRACT_WORDS, then return word
    If company name does't include '公司' or include word in EXCLUDE_WORDS, then trim company name
    :param company_name:
    :return:
    """
    # 注意 EXTRACT_WORDS 中字段有优先顺序，从左到右
    for _word in EXTRACT_WORDS:
        if company_name.find(_word) != -1:
            return _word

    if not re_full_company_name(company_name) or re_remove_sub_company(company_name):
        for _word in EXCLUDE_WORDS:
            if company_name.find(_word) != -1:
                return trim_string(company_name, _word)
    return company_name


def pre_trim_company_name(company_name):
    if company_name.find('公司') != -1:
        pre_company_name = trim_string(company_name, '公司')
    elif company_name.find('银行') != -1:
        pre_company_name = trim_string(company_name, '银行')
    elif company_name.find('集团') != -1:
        pre_company_name = trim_string(company_name, '集团')
    else:
        pre_company_name = company_name

    return pre_company_name


def mid_trim_company_name(company_name):
    """
    Remove sub company name that include '支' or '分'
    Check keyword in Province by using jieba
    Recursive get adjacent remove_words that consist of keywords
    :param company_name:
    :return:
    """
    if re_remove_sub_company(company_name):
        # company_name = re.sub(r'(省|自治区|市|县|区|州)', '', company_name)

        company_name_list = list(jieba.lcut(company_name))
        company_name_list_len = len(company_name_list)

        if company_name_list_len > 1:
            company_name_list.reverse()
            for idx in range(0, company_name_list_len - 1):
                _keyword = search_province_word(company_name_list[idx])
                if _keyword:
                    company_name = trim_string(company_name, _keyword, start=2, reserve=False)
                    if re_remove_sub_company(company_name):
                        continue
                    else:
                        return company_name

                # if company_name_list[idx] in PROVINCE:
                #     # remove_words = recursive_get_keyword(company_name_list, company_name_list_len, idx)
                #     # mid_company_name = trim_string(company_name, remove_words, start=2, reserve=False)
                #     company_name = trim_string(company_name, company_name_list[idx], start=2, reserve=False)
                #     if re_remove_sub_company(company_name):
                #         continue
                #     else:
                #         return company_name
                # else:
                #     _keyword = re.sub(r'(省|自治区|市)', '', company_name_list[idx])
                #     if _keyword in PROVINCE:
                #         company_name = trim_string(company_name, _keyword, start=2, reserve=False)
                #         if re_remove_sub_company(company_name):
                #             continue
                #         else:
                #             return company_name
                #     else:
                #         _keyword = re.sub(r'(县|区|州)', '', company_name_list[idx])
                #         if _keyword in PROVINCE:
                #             company_name = trim_string(company_name, _keyword, start=2, reserve=False)
                #             if re_remove_sub_company(company_name):
                #                 continue
                #             else:
                #                 return company_name

            return company_name
        else:
            return company_name
    else:
        return company_name


def search_province_word(key_word):
    if key_word in PROVINCE:
        return key_word
    elif len(key_word) > 1:
        for word in list(jieba.lcut(key_word, cut_all=True)):
            if len(word) > 1:
                if word in PROVINCE:
                    return word
    else:
        return


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

    print("***" * 30)
    rows = xls.generate_row_object_iterator(check_file=False, sheet_name='listing', start_point=2, end_point=10000)

    for i in rows:
        print(i.position, i.column_value.get('公司'), extract_company_name(i.column_value.get('公司')))
    print("###" * 30)

    #####################################################################################################

    # print("###" * 30)
    # keyword = '人保寿险博州中支公司'
    # result = extract_company_name(keyword)
    # print(f"result: {result}")
    # print("###" * 30)
