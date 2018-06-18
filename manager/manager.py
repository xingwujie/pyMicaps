# -*- coding:utf-8 -*-
# @author: ZHU Feng
# @Xinxiang Meteorological Bureau

from datetime import datetime, timedelta
import os
import json
from bidict import bidict


def latest_start_predict_time(now=None, year_label='y'):
    """
    根据当前时间返回最近起报时间，默认格式为'yymmddhh'
    :param now: 当前时间，datetime.datetime类型，默认为当前计算机系统的时间
    :param year_label: 指定返回时间的年份的格式，默认为两位数年份'yy',也可以指定为'Y',则返回四位数年份'yyyyy'
    :return: 字符串格式的最近起报时间，'yymmddhh'或者'yyyymmddhh'
    """
    if not now:
        now = datetime.now()
    today = now.strftime('%'+year_label+'%m%d')
    now_time = now.strftime('%'+year_label+'%m%d%H')
    if now_time < today + "12":
        yesterday = now - timedelta(days=1)
        start_predict = yesterday.strftime('%'+year_label+'%m%d') + '20'
    else:
        start_predict = today + '08'
    return start_predict


def parse_time_resolution(s):
    for i in s.split(','):
        start, end, interval = [int(each) for each in i.split()]
        for j in range(start, end + interval, interval):
            yield '%03d' % int(j)


def mdfs2mds(path, forward=True):
    models_maps = bidict({'ECMWF_HR': 'ECMWF_HR',
                          'GRAPES_GFS': 'GRAPES_GFS',
                          'GRAPES_MESO_HR': 'GRAPES_MESO',
                          'T639': 'T639_HR',
                          'GERMAN_HR': 'GERMAN_HR',
                          'JAPAN_MR': 'JAPAN_HR'
                          })
    with open('data/mdfs_mds_maps.json', 'r', encoding='utf-8') as f:
        maps = json.load(f)

    split_path = os.path.normpath(path).split(os.path.sep)

    if forward:
        if len(split_path) == 1:
            return models_maps.get(split_path[0])
        elif len(split_path) == 2:
            model, first_dir = split_path

            to_model = models_maps.get(model)
            if not to_model:
                return None

            to_first_dir = maps[model]['first_dirs_map'].get(first_dir)

            if to_first_dir:
                mdfs_second_dirs = maps[model]['mdfs_second_dirs'].get(first_dir)
                mds_second_dir = maps[model]['mds_second_dirs'].get(to_first_dir)
                # 当mdfs已经是最终目录的时候，应该映射到mds的最终目录
                if not mdfs_second_dirs and mds_second_dir and len(mds_second_dir) == 1:
                    to_second_dir = mds_second_dir[0]
                    return os.path.join(to_model, to_first_dir, to_second_dir)
                elif not mdfs_second_dirs and mds_second_dir and len(mds_second_dir) > 1:
                    raise Exception('映射目录出错')  # 这种情况不应该出现
                else:
                    return os.path.join(to_model, to_first_dir)
            else:
                return None

        elif len(split_path) == 3:
            model, first_dir, second_dir = split_path
            to_model = models_maps.get(model)
            if not to_model:
                return None

            # 特殊映射目录，比如TMP/2M_ABOVE_GROUND与TMP_2M/2,
            link = os.path.normpath(os.path.join(first_dir, second_dir)).replace(os.path.sep, '/')
            to_first_second = maps[model]['first_dirs_map'].get(link)
            if to_first_second:
                return os.path.join(to_model, to_first_second)
            # 非特殊映射
            else:
                to_first_dir = maps[model]['first_dirs_map'].get(first_dir)
                if not to_first_dir:
                    return None
                mdfs_second_dirs = maps[model]['mdfs_second_dirs'].get(first_dir)
                mds_second_dirs = maps[model]['mds_second_dirs'].get(to_first_dir)

                # mdfs二级目录只有一层
                if mdfs_second_dirs and len(mdfs_second_dirs) == 1 and second_dir == mdfs_second_dirs[0]:
                    if not mds_second_dirs:  # 对应mds没有一层的二级目录
                        to_second_dir = ''
                    elif mds_second_dirs and len(mds_second_dirs) == 1:  # 对应mds有一层的二级目录
                        to_second_dir = mds_second_dirs[0]
                    else:
                        raise Exception('映射目录出错')
                    return os.path.join(to_model, to_first_dir, to_second_dir)
                # mdfs二级目录有多层
                elif mdfs_second_dirs and second_dir in mdfs_second_dirs and second_dir in mds_second_dirs:
                    return os.path.join(to_model, to_first_dir, second_dir)
                else:
                    return None
        else:
            return None

    else:  # 从mds映射到mdfs
        if len(split_path) == 1:
            return models_maps.inv.get(split_path[0])
        elif len(split_path) == 2:
            model, first_dir = split_path

            to_model = models_maps.inv.get(model)
            if not to_model:
                return None

            to_first_dir = bidict(maps[to_model]['first_dirs_map']).inv.get(first_dir)

            if to_first_dir:
                mds_second_dirs = maps[to_model]['mds_second_dirs'].get(to_first_dir)
                mdfs_second_dir = maps[to_model]['mdfs_second_dirs'].get(first_dir)
                # 当mds已经是最终目录的时候，应该映射到mdfs的最终目录
                if not mds_second_dirs and mdfs_second_dir and len(mdfs_second_dir) == 1:
                    to_second_dir = mdfs_second_dir[0]
                    return os.path.join(to_model, to_first_dir, to_second_dir)
                elif not mds_second_dirs and mdfs_second_dir and len(mdfs_second_dir) > 1:
                    raise Exception('映射目录出错')  # 这种情况不应该出现
                else:
                    return os.path.join(to_model, to_first_dir)
            else:
                return None

        elif len(split_path) == 3:
            model, first_dir, second_dir = split_path
            to_model = models_maps.inv.get(model)
            if not to_model:
                return None

            # 特殊映射目录，比如TMP/2M_ABOVE_GROUND与TMP_2M/2,
            link = os.path.normpath(os.path.join(first_dir, second_dir)).replace(os.path.sep, '/')
            to_first_second = bidict(maps[to_model]['first_dirs_map']).inv.get(link)
            if to_first_second:
                return os.path.join(to_model, to_first_second)
            # 非特殊映射
            else:
                to_first_dir = bidict(maps[to_model]['first_dirs_map']).inv.get(first_dir)
                if not to_first_dir:
                    return None

                mds_second_dirs = maps[to_model]['mds_second_dirs'].get(first_dir)
                mdfs_second_dirs = maps[to_model]['mdfs_second_dirs'].get(to_first_dir)

                # mds二级目录只有一层
                if mds_second_dirs and len(mds_second_dirs) == 1 and second_dir == mds_second_dirs[0]:
                    if not mdfs_second_dirs:  # 对应mdfs没有一层的二级目录
                        to_second_dir = ''
                    elif mdfs_second_dirs and len(mdfs_second_dirs) == 1:  # 对应的mdfs有一层的二级目录
                        to_second_dir = mdfs_second_dirs[0]
                    else:  # 这种情况不应该出现
                        raise Exception('映射目录出错')
                    return os.path.join(to_model, to_first_dir, to_second_dir)
                # mds二级目录有多层
                elif mds_second_dirs and second_dir in mds_second_dirs and second_dir in mdfs_second_dirs:
                    return os.path.join(to_model, to_first_dir, second_dir)
                else:
                    return None

        else:
            return None


def mds2mdfs(path):
    return mdfs2mds(path, False)
