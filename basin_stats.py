# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 07:49:12 2020

@author: buriona
"""

import re
import json
import pytz
import logging
from os import path, makedirs
from datetime import datetime as dt
from logging.handlers import TimedRotatingFileHandler
from requests import get as r_get

STATIC_URL = f'https://www.usbr.gov/uc/water/hydrodata/assets'
NRCS_CHARTS_URL = 'https://www.nrcs.usda.gov/Internet/WCIS/basinCharts/POR'
MST = pytz.timezone('MST')

def create_log(path='ff_gen.log'):
    logger = logging.getLogger('ff_gen rotating log')
    logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(
        path,
        when="W6",
        backupCount=1
    )

    logger.addHandler(handler)

    return logger

def print_and_log(log_str, logger=None):
    print(log_str)
    if logger:
        logger.info(log_str)

def get_nrcs_basin_stat(basin_name, huc_level='2', data_type='wteq', 
                        logger=None):
    
    stat_type_dict = {'wteq': 'Median', 'prec': 'Average'}
    url = f'{NRCS_CHARTS_URL}/{data_type.upper()}/assocHUC{huc_level}/{basin_name}.html'
    try:
        response = r_get(url)
        if not response.status_code == 200:
            print_and_log(
                f'      Skipping {basin_name} {data_type.upper()}, NRCS does not publish stats.',
                logger
            )
            return 'N/A'
        html_txt = response.text
        stat_type = stat_type_dict.get(data_type, 'Median')
        regex = f"(?<=% of {stat_type} - )(.*)(?=%<br>%)"
        swe_re = re.search(regex, html_txt, re.MULTILINE)
        stat = html_txt[swe_re.start():swe_re.end()]
    except Exception as err:
        print_and_log(
            f'      Error gathering data for {basin_name} - {err}',
            logger
        )
        stat = 'N/A'
    return stat

def get_huc_nrcs_stats(huc_level='6', try_all=False, export_dirs=[], 
                       logger=None):
    
    print_and_log(f'  Getting NRCS stats for HUC{huc_level}...', logger)
    curr_mst = dt.now(MST).strftime('%b %I %Y %H %p $Z')
    data_types = ['prec', 'wteq']
    index_pg_urls = [f'{NRCS_CHARTS_URL}/{i.upper()}/assocHUC{huc_level}' 
                     for i in data_types]
    index_pg_resps = [r_get(i) for i in index_pg_urls]
    index_pg_codes = [i.status_code for i in index_pg_resps]
    if not set(index_pg_codes) == set([200]):
        print_and_log(
            f'  Could not download index file(s) - {index_pg_urls}, trying all basins...',
            logger
        )
        try_all = True
        index_page_strs = ['' for i in index_pg_resps]
    else:
        index_page_strs = [i.text for i in index_pg_resps]
    topo_json_path = f'./gis/HUC{huc_level}.topojson'
    with open(topo_json_path, 'r') as tj:
        topo_json = json.load(tj)
    huc_str = f'HUC{huc_level}'
    swe_stat_dict = {}
    prec_stat_dict = {}
    topo_attrs = topo_json['objects'][huc_str]['geometries']
    for attr in topo_attrs:
        props = attr['properties']
        huc_name = props['Name']
        file_name = f'href="{huc_name.replace(" ", "%20")}.html"'
        if try_all or file_name in index_page_strs[0]:
            print_and_log(
                f'  Getting NRCS PREC stats for {huc_name}...', 
                logger
            )
            prec_stat = get_nrcs_basin_stat(
                huc_name, 
                huc_level=huc_level, 
                data_type='prec',
                logger=logger
            )
            props['prec_percent'] = prec_stat
            props['prec_updt'] = curr_mst
            prec_stat_dict[huc_name] = prec_stat
        else:
            props['prec_percent'] = "N/A"
            prec_stat_dict[huc_name] = "N/A"
        if try_all or file_name in index_page_strs[1]:
            print_and_log(
                f'  Getting NRCS WTEQ stats for {huc_name}...',
                logger
            )
            swe_stat = get_nrcs_basin_stat(
                huc_name, 
                huc_level=huc_level, 
                data_type='wteq',
                logger=logger
            )
            props['swe_percent'] = swe_stat
            props['swe_updt'] = curr_mst
            swe_stat_dict[huc_name] = swe_stat
        else:
            props['swe_percent'] = "N/A"
            swe_stat_dict[huc_name] = "N/A"
    topo_json['objects'][huc_str]['geometries'] = topo_attrs
    
    geo_json_path = f'./gis/HUC{huc_level}.geojson'
    with open(geo_json_path, 'r') as gj:
        geo_json = json.load(gj)
    geo_features = geo_json['features']
    for geo_feature in geo_features:
        geo_props = geo_feature['properties']
        huc_name = geo_props['Name']
        geo_props['prec_percent'] = prec_stat_dict.get(huc_name, 'N/A')
        geo_props['swe_percent'] = swe_stat_dict.get(huc_name, 'N/A')
        geo_props['prec_updt'] = curr_mst
        geo_props['swe_updt'] = curr_mst
        
    geo_json['features'] = geo_features
    geo_export_paths = [geo_json_path]
    topo_export_paths = [topo_json_path]
    for export_dir in export_dirs:
        if path.isdir(export_dir):
            print_and_log(
                f'Exporting to alt dir: {export_dir}', 
                logger
            )
            add_geo_path = path.join(
                export_dir, 
                f'HUC{huc_level}.geojson'
            )
            geo_export_paths.append(add_geo_path)
            add_topo_path = path.join(
                export_dir, 
                f'HUC{huc_level}.topojson'
            )
            topo_export_paths.append(add_topo_path)
        else:
            print_and_log(
                f'Cannot export to alt dir: {export_dir}, does not exist',
                logger
            )
    for export_path in geo_export_paths:
        with open(export_path, 'w') as tj:
            json.dump(geo_json, tj)
    for export_path in topo_export_paths:
        with open(export_path, 'w') as tj:
            json.dump(topo_json, tj)

def update_gis_files(huc_level, logger=None, export_dirs=[]):
    try:
        gis_str = (
            f'Updating HUC{huc_level} '
            f'GIS files with current NRCS data...'
        )
        print_and_log(gis_str, logger)
        get_huc_nrcs_stats(
            huc_level=huc_level, 
            export_dirs=export_dirs,
            logger=logger
        )
        gis_str = (
            f'  Successfully updated HUC{huc_level} '
            f'GIS files with current NRCS data.\n'
        )
        print_and_log(gis_str, logger)
    except Exception as err:
        gis_str = (
            f'  Failed to update HUC{huc_level} '
            f'GIS files with current NRCS data - {err}\n'
        )
        print_and_log(gis_str, logger)
            
if __name__ == '__main__':
    
    import argparse
    
    cli_desc = 'Updates topo/geojson HUC layers in gis folder with NRCS basin stats, can also export files to other locations'
    parser = argparse.ArgumentParser(description=cli_desc)
    parser.add_argument("-V", "--version", help="show program version", action="store_true")
    parser.add_argument("-l", "--level", help="Updates single HUC level or multiple seperated by commas. Will update all without this flag")
    parser.add_argument("-e", "--export", help="Additional path to write HUC layers to.", action='append')
    
    args = parser.parse_args()
    
    if args.version:
        print('basin_stats.py v1.0')
    
    valid_hucs = ['2', '4', '6', '8']
    if args.level:
        huc_levels = args.level.split(',')
        huc_levels[:] = [str(i) for i in huc_levels if str(i) in valid_hucs]
    else:
        huc_levels = ['2', '4', '6', '8']
    
    export_dirs = []
    if args.export:
        export_dirs = args.export
    
    this_dir = path.dirname(path.realpath(__file__))
    logger = create_log(path.join(this_dir, 'basin_stats.log'))
    for huc_level in huc_levels:
        
        gis_dir = path.join(this_dir, 'gis')
        makedirs(gis_dir, exist_ok=True)
        update_gis_files(huc_level, logger=logger, export_dirs=export_dirs)