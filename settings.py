# -*- coding:utf-8 -*-
"""
settings for vhalldata project.
"""
from province import PROVINCE

# ThreadPoolExecutor max_workers
MAX_WORKERS = 4

# Thread task execution waiting time(s). as_completed
TASK_WAITING_TIME = 3600 * 1

# Dictionary file
DICT_FILE = '会员单位名单.xlsx'

# Exclude words
# EXCLUDE_WORDS = ['中国', '股份', '有限', '公司', '集团', '销售', '代理', '经纪', '销售', '公估', '服务']
EXCLUDE_WORDS = ['营业部', '中心', '支公司', '分公司', '客服部', '客服部', '客服处', '经纪', '服务', '北京', '天津', '上海', '重庆', '河北', '山西', '辽宁',
                 '吉林', '黑龙江', '江苏', '浙江', '安徽', '福建', '江西', '山东', '河南', '湖北', '湖南', '广东', '海南', '四川', '贵州', '云南', '陕西',
                 '甘肃', '青海', '台湾', '内蒙古', '广西', '西藏', '宁夏', '新疆']
