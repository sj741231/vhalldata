# -*- coding:utf-8 -*-
__author__ = 'shijin'

import re
from pprint import pprint
from utils.util_xlsx import HandleXLSX
from utils.util_logfile import nlogger, flogger, slogger, traceback
from utils.util_re import re_bank, re_agency_company, re_appraisal_company, re_economic_company, re_insurance, \
    re_sale_company


class Samples(object):
    all_name = {}
    all_full_name = {}
    all_abbr_name = {}

    bank_name = {}
    bank_full_name = {}
    bank_abbr_name = {}

    insurance_appraisal_name = {}
    insurance_appraisal_full_name = {}
    insurance_appraisal_abbr_name = {}

    insurance_economic_name = {}
    insurance_economic_full_name = {}
    insurance_economic_abbr_name = {}

    insurance_agency_name = {}
    insurance_agency_full_name = {}
    insurance_agency_abbr_name = {}

    insurance_sale_name = {}
    insurance_sale_full_name = {}
    insurance_sale_abbr_name = {}

    insurance_company_name = {}
    insurance_company_full_name = {}
    insurance_company_abbr_name = {}

    related_institutions_name = {}
    related_institutions_full_name = {}
    related_institutions_abbr_name = {}

    # insurance_company_type = {
    #     'insurance_economic': {'full': 'insurance_economic_full_name',
    #                            'abbr': 'insurance_economic_abbr_name'},
    #     'insurance_agency': {'full': 'insurance_agency_full_name',
    #                          'abbr': 'insurance_agency_abbr_name'},
    #     'insurance_sale': {'full': 'insurance_sale_full_name',
    #                        'abbr': 'insurance_sale_abbr_name'},
    #     'insurance_appraisal': {'full': 'insurance_appraisal_full_name',
    #                             'abbr': 'insurance_appraisal_abbr_name'},
    #     'related_institutions': {'full': 'related_institutions_full_name',
    #                              'abbr': 'related_institutions_abbr_name'},
    # }

    def get_property(self, property_name):
        if hasattr(self, property_name):
            return getattr(self, property_name)
        else:
            setattr(self, property_name, {})
            return getattr(self, property_name)

    def set_property(self, property_name, property_value):
        setattr(self, property_name, property_value)

    def recursive_search_key_value(self, property_name, search_key):
        """
        Recursive search key value in instance property that is a dict
        :param property_name: property name, string
        :param search_key: keyword for search
        :return: search result or {}
        """
        assert isinstance(property_name, str) and str(
            property_name).strip(), f"Samples object property must be dict and can't be empty"
        assert isinstance(search_key, str) and str(
            search_key).strip(), f"Samples object property must be string and can't be empty"

        _sample_obj_property = self.get_property(property_name).get(search_key, {})

        if not _sample_obj_property or _sample_obj_property.get('termination'):
            return _sample_obj_property
        else:
            _property_name, _search_key = _sample_obj_property.popitem()
            return self.recursive_search_key_value(_property_name, _search_key)


def get_samples_object(row_object_iterator, **kwargs):
    """

    :param row_object_iterator:
    :param kwargs:
    :return:
    """
    samples_instance = Samples()
    for row_object in row_object_iterator:
        samples_instance = add_sample_object_property(samples_instance, row_object, **kwargs)
    return samples_instance


def get_name_type(row_object):
    _company_name = row_object.column_value.get('单位名称')
    if re_bank(_company_name):
        return 'bank'
    elif re_appraisal_company(_company_name):
        return 'insurance_appraisal'
    elif re_economic_company(_company_name):
        return 'insurance_economic'
    elif re_agency_company(_company_name):
        return 'insurance_agency'
    elif re_sale_company(_company_name):
        return 'insurance_sale'
    elif re_insurance(_company_name):
        return 'insurance_company'
    else:
        return 'related_institutions'


def simplify_company_name(full_name):
    brief_name = re.split('有限公司|有限责任公司|（集团）公司|股份有限公司|（集团）股份有限公司|公司', full_name)[0]
    return brief_name


def add_sample_object_property(samples_object, row_object, **kwargs):
    try:
        full_name = row_object.column_value.get('单位名称')
        abbr_name = row_object.column_value.get('简称')
        assert str(full_name).strip(), f"单位名称 can't be empty"
        assert str(abbr_name).strip(), f"简称 can't be empty"
        brief_name = simplify_company_name(full_name)

        # sort name type
        _name_type = get_name_type(row_object)

        # structure Samples property name
        _property_name = f'{_name_type}_name'
        _property_full_name = f'{_name_type}_full_name'
        _property_abbr_name = f'{_name_type}_abbr_name'

        _property_name_value = samples_object.get_property(_property_name)
        _property_full_name_value = samples_object.get_property(_property_full_name)
        _property_abbr_name_value = samples_object.get_property(_property_abbr_name)

        # add source
        _property_full_name_value[full_name] = {'termination': True,
                                                'full_name': row_object.column_value.get('单位名称'),
                                                'company_type': row_object.column_value.get('单位类别')}
        samples_object.set_property(_property_full_name, _property_full_name_value)

        # add source reference
        _property_name_value[full_name] = {_property_full_name: full_name}
        samples_object.set_property(_property_name, _property_name_value)

        samples_object.all_full_name[full_name] = {_property_full_name: full_name}
        samples_object.all_name[full_name] = {_property_full_name: full_name}

        _property_abbr_name_value[abbr_name] = {_property_full_name: full_name}
        samples_object.set_property(_property_abbr_name, _property_abbr_name_value)

        _property_name_value[abbr_name] = {_property_full_name: full_name}
        samples_object.set_property(_property_name, _property_name_value)

        samples_object.all_abbr_name[abbr_name] = {_property_full_name: full_name}
        samples_object.all_name[abbr_name] = {_property_full_name: full_name}

        if brief_name != abbr_name:
            _property_abbr_name_value[brief_name] = {_property_full_name: full_name}
            samples_object.set_property(_property_abbr_name, _property_abbr_name_value)

            _property_name_value[brief_name] = {_property_full_name: full_name}
            samples_object.set_property(_property_name, _property_name_value)

            samples_object.all_abbr_name[brief_name] = {_property_full_name: full_name}
            samples_object.all_name[brief_name] = {_property_full_name: full_name}

        return samples_object
    except Exception as e:
        nlogger.error('{fn} Undefined error: {e}'.format(fn='add_sample_object_property', e=traceback.format_exc()))
        print(f'Undefined error: {repr(e)}')
        raise


if __name__ == "__main__":
    print("###" * 30)

    filename = '会员单位名单.xlsx'
    xls = HandleXLSX(filename)
    # rows = xls.generator_rows_value_iterator(sheet_name='listing', start_point=1, end_point=10)
    #
    # for i in rows:
    #     print("row: ", i)

    print("###" * 30)
    rows = xls.generate_row_object_iterator(check_file=False, sheet_name='dict', start_point=255, end_point=265)

    samples_object = get_samples_object(row_object_iterator=rows)
    print("***" * 30)
    print("samples_object: ", samples_object)
    # print("***" * 30)
    # print("samples_object: ", dir(samples_object))
    # print("***" * 30)
    # pprint("_class__.__dict__ : ")
    # pprint(samples_object.__class__.__dict__)
    # print("samples_object.__dict__ : ", samples_object.__dict__)
    print("***" * 30)
    pprint("samples_object.name: ")
    pprint(samples_object.all_name)
    pprint(samples_object.all_full_name)
    pprint(samples_object.all_abbr_name)
    print("***" * 30)
    pprint("samples_object.bank: ")
    pprint(samples_object.bank_name)
    pprint(samples_object.bank_full_name)
    pprint(samples_object.bank_abbr_name)
    print("***" * 30)
    pprint("samples_object.insurance_company: ")
    pprint(samples_object.insurance_company_name)
    pprint(samples_object.insurance_company_full_name)
    pprint(samples_object.insurance_company_abbr_name)
    print("***" * 30)
    pprint("samples_object.insurance_appraisal: ")
    pprint(samples_object.insurance_appraisal_name)
    pprint(samples_object.insurance_appraisal_full_name)
    pprint(samples_object.insurance_appraisal_abbr_name)
    print("***" * 30)
    pprint("samples_object.insurance_economic: ")
    pprint(samples_object.insurance_economic_name)
    pprint(samples_object.insurance_economic_full_name)
    pprint(samples_object.insurance_economic_abbr_name)
    print("***" * 30)
    pprint("samples_object.insurance_agency: ")
    pprint(samples_object.insurance_agency_name)
    pprint(samples_object.insurance_agency_full_name)
    pprint(samples_object.insurance_agency_abbr_name)
    print("***" * 30)
    pprint("samples_object.insurance_sale: ")
    pprint(samples_object.insurance_sale_name)
    pprint(samples_object.insurance_sale_full_name)
    pprint(samples_object.insurance_sale_abbr_name)
    print("***" * 30)
    pprint("samples_object.related_institutions: ")
    pprint(samples_object.related_institutions_name)
    pprint(samples_object.related_institutions_full_name)
    pprint(samples_object.related_institutions_abbr_name)
    print("###" * 30)

    result = samples_object.recursive_search_key_value('insurance_sale_abbr_name', '长安保险销售')
    print(f"result: {result}")
    print("###" * 30)

