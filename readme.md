## 项目说明

1. 处理vhall收集用户报名数据，匹配公司名称：   
    a.创建查询公司名称字典表  
    b.清洗源文件中公司名称 
    c.进行公司名称比对 
  

### 文档说明：
* 依赖包清单 *  
	1. 环境依赖包清单在项目根下doc目录下文件。
* 批量安装依赖包 *
	2. pip3.9 install -r requirements.txt

### 配置文件：
* 配置文件 *
    1. 配置文件在:/vhalldata/settings.py    

### 执行方式：
1. 命令行帮助 *
    a. python3.9 handle.py --help
    b. python3.9 handle.py -h
2. 执行命令 *
    a. python3.9 handle.py --file 'vid-20210214.xlsx' --sheet 'listing' --start 2 --end 4
    b. python3.9 handle.py -check --file 'vid-20210214.xlsx' --sheet 'listing' --start 2 --end 4
    c. python3.9 handle.py -f 'vid-20210214.xlsx' -t 'listing' -s 2 -e 4
    d. python3.9 handle.py -c -f 'vid-20210214.xlsx' -t 'listing' -s 2 -e 4
