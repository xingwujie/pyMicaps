# -*- coding:utf-8 -*-
# @author: ZHU Feng
# @Xinxiang Meteorological Bureau

from cma.gds import GDSDataService
import time, os
import json
from datetime import datetime, timedelta


# 获取名称中文描述字典
# 该文件由MICAPS4软件提供，MICAPS4软件的data/ModeDataDictionary.txt
with open('ModeDataDictionary.txt', 'r', encoding='utf-8') as f:
    DATA_DESCRIPTION_DICT = {line.split()[0]: line.split()[1] for line in f if line.strip()}



def get_MDFS_model_dirs(model_names):
    # todo 对GRPAES_MESO_HR的RAIN01和SNOW01有误
    """
    获取mdfs中各个模式的目录，并写到*_MDFS.txt文件中，*表示模式名称
    :param model_names: 模式名列表
    :return: 无
    """
    with GDSDataService() as gds:
        for model_name in model_names:
            with open(model_name + '_MDFS.txt', 'w', encoding='utf-8') as f:
                # for i in gds.walk(model_name):
                for root, dirs in gds.walk(model_name):
                    first_dir = root.split('/')[1]
                    if dirs:  # 有两级目录
                        second_dirs = '/'.join(map(str, sorted([j for j in dirs if not j.isdigit()])
                                                   + sorted([int(j) for j in dirs if j.isdigit()])))
                        f.write(DATA_DESCRIPTION_DICT[first_dir] + ' ' * 4 + first_dir + ' ' * 4 + second_dirs + '\n')
                    elif len(root.split('/')) < 3:  # 只有一级目录
                        f.write(DATA_DESCRIPTION_DICT[first_dir] + ' ' * 4 + first_dir + '\n')


def get_MDS_model_dirs(model_names, MDS_root):
    """
    获取MDS文件系统中各个模式的目录，并写到*_MDS.txt文件中，*表示模式名称
    :param model_names: 模式名列表
    :param MDS_root: MDS文件根目录
    :return: 无
    """
    for model in model_names:
        with open(model + '_MDS.txt', 'w', encoding='utf-8') as f:
            model_root = os.path.join(MDS_root, model)
            for first_dir in os.listdir(model_root):
                if first_dir == 'physic':
                    continue

                dirs = []
                for second_dir in os.listdir(os.path.join(model_root, first_dir)):
                    if os.path.isfile(os.path.join(model_root, first_dir, second_dir)):
                        f.write(first_dir + '\n')
                        break
                    else:
                        dirs.append(second_dir)
                else:
                    second_dirs = '/'.join(map(str, sorted([j for j in dirs if not j.isdigit()])
                                               + sorted([int(j) for j in dirs if j.isdigit()])))
                    f.write(first_dir + ' ' * 4 + second_dirs + '\n')


def setup_mdfs2mds_maps(models):
    """
    建立mdfs目录和mds目录之间的映射关系，映射关系保存在'mdfs_mds_maps.json'文件中
    :param models:
    :return:
    """
    mdfs_mds_maps = {}
    for model in models:
        mdfs_mds_maps[model] = {}
        map = {}
        mdfs_second_dirs = {}
        mds_second_dirs = {}
        with open(model + '_MDFS-MDS.txt', 'r', encoding='utf-8') as f:
            for line in f:
                first_dir_mdfs, second_dirs_mdfs, first_dir_mds, second_dirs_mds = line.strip().split(',')
                map[first_dir_mdfs] = first_dir_mds
                if second_dirs_mdfs:
                    mdfs_second_dirs[first_dir_mdfs] = [i for i in second_dirs_mdfs.split('/')]
                if second_dirs_mds:
                    mds_second_dirs[first_dir_mds] = [i for i in second_dirs_mds.split('/')]

        mdfs_mds_maps[model]['first_dirs_map'] = map
        mdfs_mds_maps[model]['mdfs_second_dirs'] = mdfs_second_dirs
        mdfs_mds_maps[model]['mds_second_dirs'] = mds_second_dirs

    with open('mdfs_mds_maps.json', 'w', encoding='utf-8') as f:
        json.dump(mdfs_mds_maps, f, indent=4, ensure_ascii=False)


def Model(name, year_label='y', time_start_index=0, backup='', download='', description=''):
    return {'name': name,
            'year_label': year_label,
            'time_start_index': time_start_index,
            'backup': backup,
            'download': download,
            'description': description
            # 'fisrt_dirs'
            }


def First_dir(name, backup='', download='', priority='', time_resolution='', spatial_resolution='', unit='',
              description=''):
    return {'name': name,
            'backup': backup,
            'download': download,
            'priority': priority,
            'time_resolution': time_resolution,
            'spatial_resolution': spatial_resolution,
            'unit': unit,
            'description': description
            # 'second_dirs'
            }


def Second_dir(name, backup='', download='', priority='', time_resolution='', spatial_resolution='', unit='',
               description=''):
    return {'name': name,
            'backup': backup,
            'download': download,
            'priority': priority,
            'time_resolution': time_resolution,
            'spatial_resolution': spatial_resolution,
            'unit': unit,
            'description': description
            }


def init_config_data(models):
    """
    根据各模式MDFS目录初始化化配置字典
    :param models: 各模式名的列表
    :return:
    """
    micaps_data = {}
    for model in models:
        model_dict = Model(model, description=DATA_DESCRIPTION_DICT.get(model, model))
        first_dirs_list = {}
        with open('%s_MDFS.txt' % model, 'r', encoding='utf-8') as f:
            for line in f:
                desc, first_dir, *second_dirs = line.strip().split()
                first_dir_dict = First_dir(first_dir, description=desc)
                if second_dirs:
                    second_dirs_list = {}
                    for each in second_dirs[0].split('/'):
                        second_dirs_list[each] = Second_dir(each, description=DATA_DESCRIPTION_DICT.get(each, ''))

                    first_dir_dict['second_dirs'] = second_dirs_list
                first_dirs_list[first_dir] = first_dir_dict
        model_dict['first_dirs'] = first_dirs_list
        micaps_data[model] = model_dict

    return micaps_data


def setup_time_resolution(micaps_data):
    """
    获取模式中各个数据的时间分辨率，并就地写到字典micaps_data中。
    时间分辨率的格式是'start1 end1 interval1[,start2 end2 interval2,……],start和end都包含，是[start, end]的闭区间
    :param micaps_data: 总的配置字典
    :return: 无
    """

    def get_time_resolution(L):
        if len(L) == 1:
            return str(L[0])
        s = ''
        start = L[0]
        interval = L[1] - L[0]
        for i in range(2, len(L)):
            if L[i] - L[i - 1] != interval:
                end = L[i - 1]
                s += '%s %s %s,' % (start, end, interval)
                start = L[i]
                interval = L[i] - L[i - 1]
            if i == len(L) - 1:
                end = L[i]
                s += '%s %s %s,' % (start, end, interval)

        return s[:-1]

    def get_L(directory, times):
        Ls = []
        with GDSDataService() as gds:
            for t in times:
                L = [int(each[-3:]) for each in gds.get_file_list(directory, t) if int(each.split('.')[1]) >= 0]
                L.sort()
                Ls.append(L)
        L = Ls[0]
        if not Ls[0] == Ls[1] == Ls[2]:
            print('error:', directory)
            print('Ls0:', Ls[0])
            print('Ls1:', Ls[1])
            print('Ls2:', Ls[2])

            if len(Ls[1]) > len(L):
                L = Ls[1]
            if len(Ls[2]) > len(L):
                L = Ls[2]
        return L

    now = datetime.now()
    times = [(now - timedelta(days=day)).strftime('%y%m%d') + '20' for day in range(1, 4)]

    for model in micaps_data:
        for first_dir, first_dir_dict in micaps_data[model]['first_dirs'].items():
            has_second_dir = False
            for second_dir in first_dir_dict.get('second_dirs', ''):
                directory = os.path.join(model, first_dir, second_dir)
                L = get_L(directory, times)
                if not L:
                    print(directory)
                # micaps_data[model]['first_dirs'][first_dir]['second_dirs'][second_dir]['time_resolution'] = get_time_resolution(L)
                first_dir_dict['second_dirs'][second_dir]['time_resolution'] = get_time_resolution(L)

                # second_dir_dict['time_resolution'] = get_time_resolution(L)
                has_second_dir = True
            if not has_second_dir:
                directory = os.path.join(model, first_dir)
                L = get_L(directory, times)
                if not L:
                    print(directory)
                # micaps_data[model]['first_dirs'][first_dir]['time_resolution'] = get_time_resolution(L)
                first_dir_dict['time_resolution'] = get_time_resolution(L)


def main_setup_config():
    models_mdfs = ['ECMWF_HR', 'ECMWF_LR', 'GRAPES_GFS', 'GRAPES_MESO_HR', 'T639', 'GERMAN_HR', 'JAPAN_MR',
                   'SHANGHAI_HR']
    models_mds = ['ECMWF_HR', 'GRAPES_GFS', 'GRAPES_MESO', 'T639_HR', 'GERMAN_HR', 'JAPAN_HR']
    # 1. 获取MDFS和MDS模式目录，写到文本文档
    # get_MDFS_model_dirs(models_mdfs)
    # get_MDS_model_dirs(models_mds, 'Y:/')

    # 2. 初始化配置字典数据
    # micaps_data = init_config_data(models_mdfs)

    # 初始化进行一些其他属性的配置
    # 3. 设置时间分辨率
    # setup_time_resolution(micaps_data)

    # 把配置字典数据写到json文件
    # with open('micaps_data.json', 'w', encoding='utf-8') as f:
    #    json.dump(micaps_data, f, indent=4, ensure_ascii=False)

    # 从文件中读取配置字典
    with open('micaps_data.json', 'r', encoding='utf-8') as f:
        micaps_data = json.load(f)


if __name__ == '__main__':
    start = time.clock()
    # ***********************测试程序*********************************"
    setup_mdfs2mds_maps(['ECMWF_HR','GERMAN_HR','JAPAN_MR'])

    # main_setup_config()
    # ***********************测试程序*********************************"
    end = time.clock()
    elapsed = end - start
    print("Time used: %.6fs, %.6fms\n" % (elapsed, elapsed * 1000))
