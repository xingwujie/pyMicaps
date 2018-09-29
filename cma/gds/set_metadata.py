# -*- coding:utf-8 -*-
# @author: ZHU Feng
# @Xinxiang Meteorological Bureau

from .gds_data_service import GDSDataService
import xml.etree.ElementTree as ET
import os, time
from datetime import datetime,timedelta



def create_data_node(gds, data_source, data_name):
    """
    创建data_source目录的xml节点
    """
    description = {}
    with open(data_source, 'r', encoding='utf-8') as f:
        for line in f:
            key, des = line.split()
            description[key] = des

    result = get_mdfs_data_dirs(gds, data_source)
    data_node = ET.Element('MODEL_DATA',
                           {'name': data_source, 'description': data_name, 'year_label': 'y', 'time_start': '0'})

    # 第一级目录
    for dir1 in result:

        dir1_elem = ET.Element('FIRST_DIR',
                               {'description': '', 'unit': '', 'time_resolution': '', 'spatial_resolution': '',
                                'download': 'True', 'backup': 'True', 'priority': 'False', 'year_label': '',
                                'time_start': ''})
        dir1_elem.text = dir1
        dir1_elem.attrib['description'] = description[dir1]

        # 需要优先下载的数据
        if dir1 in ('WIND', 'HGT', 'TMP', 'PRMSL', 'RH'):
            dir1_elem.attrib['priority'] = 'True'

        # 一些可以通过二次计算得到的预报只下载，不备份   # todo 哪些数据可以二次计算获得 需要进一步明确
        if dir1[-2:] in ('03', '06', '12', '24'):
            dir1_elem.attrib['backup'] = 'False'
        if dir1 in ('DEPR', 'CIN', 'CONDENSATION_LAYER_PRESSURE', 'K_INDEX', 'LI', 'SHOWALTER_INDEX'):
            dir1_elem.attrib['backup'] = 'False'

        # 按季节考虑的要素
        # 冬季下载但不备份
        if 'FREEZING_RAIN' in dir1 or 'SNOW' in dir1:
            dir1_elem.attrib['download'] = '10,11,12,1,2,3,4'
        # 夏季下载但不备份
        if 'RAINC' in dir1 or 'RAIN_LARGE_SCALE' in dir1:
            dir1_elem.attrib['download'] = '4,5,6,7,8,9,10'
        # 冬季既下载又备份
        if dir1 in ('SDEN', 'SNOD', 'ASNOW', 'FZRA'):
            dir1_elem.attrib['backup'] = '10,11,12,1,2,3,4'
            dir1_elem.attrib['download'] = '10,11,12,1,2,3,4'
        # 夏季既下载又备份
        if dir1 in ('ACPCP', 'APCP_LARGE_SCALE', 'RADAR_COMBINATION_REFLECTIVITY', 'RADAR_REFLECTIVITY'):
            dir1_elem.attrib['backup'] = '4,5,6,7,8,9,10'
            dir1_elem.attrib['download'] = '4,5,6,7,8,9,10'

        # 一些永不需要下载和备份的数据
        if dir1 in (
                # EC
                'SPECIFIC_CLOUD_ICE_WATER_CONTENT', 'SPECIFIC_CLOUD_LIQUID_WATER_CONTENT', 'FRACTION_OF_CLOUD_COVER',
                'UGRD_100M', 'VGRD_100M', '10_METRE_WIND_GUST_IN_THE_LAST_6_HOURS', 'UGRD_10M', 'VGRD_10M', 'UGRD',
                'VGRD', 'OROGRAPHY', 'ALBEDO', 'MAXIMUM_TEMPERATURE_AT_2_METRES_IN_THE_LAST_6_HOURS',
                'MINIMUM_TEMPERATURE_AT_2_METRES_IN_THE_LAST_6_HOURS', 'NATURAL_LOGARITHM_OF_PRESSURE_IN_PA',
                # GRAPES_GFS
                'GRAUPEL',
                # SHANHHAI_HR
                'LATITUDE_-90-90', 'LONGITUDE_0-360'):
            dir1_elem.attrib['backup'] = 'False'
            dir1_elem.attrib['download'] = 'False'

        if dir1 == 'ALBEDO' and data_source == 'GRAPES_GFS':
            dir1_elem.attrib['backup'] = 'True'
            dir1_elem.attrib['download'] = 'True'

        # 处理第二级目录
        for dir2 in result[dir1]:

            dir2_elem = ET.Element('SECOND_DIR',
                                   {'description': '', 'unit': '', 'time_resolution': '', 'spatial_resolution': '',
                                    'download': 'True', 'backup': 'True'})
            dir2_elem.text = dir2

            if dir1 in (
                    # EC下某些不保存、不下载的要素的次要目录的相应属性也设置为false
                    'SPECIFIC_CLOUD_ICE_WATER_CONTENT', 'SPECIFIC_CLOUD_LIQUID_WATER_CONTENT',
                    'FRACTION_OF_CLOUD_COVER',
                    'UGRD', 'VGRD', 'NATURAL_LOGARITHM_OF_PRESSURE_IN_PA',
                    # GRAPES_GFS
                    'GRAUPEL'):
                dir2_elem.attrib['backup'] = 'False'
                dir2_elem.attrib['download'] = 'False'

            # 层次太多，略过一些层次，既不保存、又不下载
            if dir2 in ('10', '20', '30', '50', '70', '125', '175', '225', '275', '350', '450', '550', '650', '750'):
                dir2_elem.attrib['backup'] = 'False'
                dir2_elem.attrib['download'] = 'False'

            if 'BELOW_GROUND' in dir2:  # 地表以下要素预报略过
                dir2_elem.attrib['backup'] = 'False'
                dir2_elem.attrib['download'] = 'False'

            dir1_elem.append(dir2_elem)

        data_node.append(dir1_elem)

    return data_node







