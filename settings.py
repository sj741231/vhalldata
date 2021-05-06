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
SAMPLES_FILE = '会员单位名单.xlsx'

# Exclude words
# EXCLUDE_WORDS = ['中国', '股份', '有限', '公司', '集团', '销售', '代理', '经纪', '销售', '公估', '服务']
EXCLUDE_WORDS = ['产险', '财险', '寿险']

EXTRACT_WORDS = ['工行', '工商银行', '建行', '建设银行', '农行', '农业银行', '中行', '中国银行', '招行', '招商银行',
                 '交行', '交通银行', '农商行', '农商银行', '农村商业银行', '邮政储蓄银行', '邮政']
