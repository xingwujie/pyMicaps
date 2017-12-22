# -*- coding:utf-8 -*-

from GDSDataService import GDSDataService
import xml.etree.ElementTree as ET
import os, time


def get_mdfs_data_dirs(gds, source, no_unclipped=True):
    result = {}
    l = list(gds.get_file_list(source))  # 第一级目录
    if no_unclipped:
        l = [i for i in l if 'UNCLIPPED' not in i]
    l.sort()

    for dir in l:
        if gds.get_latest_data_name(os.path.join(source, dir), '*.024') != "" or gds.get_latest_data_name(
                os.path.join(source, dir), '*.000') != "":  # 没有第二级目录

            result[dir] = ''
        else:
            listdir = gds.get_file_list(os.path.join(source, dir))  # 第二级目录
            list_dir = list(listdir)

            if no_unclipped:
                list_dir = [i for i in list_dir if 'UNCLIPPED' not in i]
            list_dir.sort()

            result[dir] = list_dir

    return result


def create_data_node(gds, data_source, data_name):
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

        # 一些可以通过二次计算得到的预报只下载，不备份 #todo 哪些数据可以二次计算获得 需要进一步明确
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
                'UGRD_100M', 'UGRD_100M', '10_METRE_WIND_GUST_IN_THE_LAST_6_HOURS', 'UGRD_10M', 'VGRD_10M', 'UGRD',
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


def write_to_xml(gds, out_file='micapsdata.xml'):
    data_nodes = list()
    data_source = {'ECMWF_HR': u'欧洲高分辨率模式', 'GRAPES_GFS': u'GRAPES全球模式',
                   'GRAPES_MESO_HR': u'GRAPES区域模式', 'T639': u'T639高分辨率模式',
                   'JAPAN_HR': u'日本高分辨率模式', 'GERMAN_HR': u'德国高分辨率模式',
                   'SHANGHAI_HR': u'华东区域模式'}
    for data in data_source:
        node = create_data_node(gds, data, data_source[data])
        data_nodes.append(node)

    root = ET.Element('MICAPSDATA')
    for node in data_nodes:
        root.append(node)

    tree = ET.ElementTree(root)
    tree.write(out_file, 'utf-8', True, short_empty_elements=False)


if __name__ == '__main__':
    with GDSDataService("10.69.72.112", 8080) as gds:
        start = time.clock()
        # ***********************测试程序*********************************"
        # dirs = get_mdfs_data_dirs(gds, 'SHANGHAI_HR')  # ECMWF_HR')
        # for key in dirs:
        #     if dirs[key] == '':
        #         print(key)
        #     else:
        #         s = '/'.join(dirs[key])
        #         print(key, s)
        write_to_xml(gds)

        # ***********************测试程序*********************************"
        end = time.clock()
        elapsed = end - start
        print("Time used: %.6fs, %.6fms\n" % (elapsed, elapsed * 1000))
