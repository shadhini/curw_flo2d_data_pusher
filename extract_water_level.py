import json
import traceback
import sys
import os
from os.path import join as path_join
from datetime import datetime, timedelta
import re
import csv

from db_adapter.logger import logger
from db_adapter.base import get_Pool
from db_adapter.curw_fcst.source import get_source_id
from db_adapter.curw_fcst.variable import get_variable_id
from db_adapter.curw_fcst.unit import get_unit_id, UnitType

COMMON_DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def read_attribute_from_config_file(attribute, config, compulsory):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :param compulsory: Boolean value: whether the attribute is must present or not in the config file
    :return:
    """
    if attribute in config and (config[attribute]!=""):
        return config[attribute]
    elif compulsory:
        logger.error("{} not specified in config file.".format(attribute))
        exit(1)
    else:
        logger.error("{} not specified in config file.".format(attribute))
        return None


def getUTCOffset(utcOffset, default=False):
    """
    Get timedelta instance of given UTC offset string.
    E.g. Given UTC offset string '+05:30' will return
    datetime.timedelta(hours=5, minutes=30))

    :param string utcOffset: UTC offset in format of [+/1][HH]:[MM]
    :param boolean default: If True then return 00:00 time offset on invalid format.
    Otherwise return False on invalid format.
    """
    offset_pattern = re.compile("[+-]\d\d:\d\d")
    match = offset_pattern.match(utcOffset)
    if match:
        utcOffset = match.group()
    else:
        if default:
            print("UTC_OFFSET :", utcOffset, " not in correct format. Using +00:00")
            return timedelta()
        else:
            return False

    if utcOffset[0] == "-":  # If timestamp in negtive zone, add it to current time
        offset_str = utcOffset[1:].split(':')
        return timedelta(hours=int(offset_str[0]), minutes=int(offset_str[1]))
    if utcOffset[0] == "+":  # If timestamp in positive zone, deduct it to current time
        offset_str = utcOffset[1:].split(':')
        return timedelta(hours=-1 * int(offset_str[0]), minutes=-1 * int(offset_str[1]))


def get_water_level_of_channels(lines, channels=None):
    """
     Get Water Levels of given set of channels
    :param lines:
    :param channels:
    :return:
    """
    if channels is None:
        channels = []
    water_levels = {}
    for line in lines[1:]:
        if line == '\n':
            break
        v = line.split()
        if v[0] in channels:
            # Get flood level (Elevation)
            water_levels[v[0]] = v[5]
            # Get flood depth (Depth)
            # water_levels[int(v[0])] = v[2]
    return water_levels


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def save_forecast_timeseries(my_adapter, my_timeseries, my_model_date, my_model_time, my_opts):
    print('EXTRACTFLO2DWATERLEVEL:: save_forecast_timeseries >>', my_opts)

    # Convert date time with offset
    date_time = datetime.strptime('%s %s' % (my_model_date, my_model_time), Constants.COMMON_DATE_TIME_FORMAT)
    if 'utcOffset' in my_opts:
        date_time = date_time + my_opts['utcOffset']
        my_model_date = date_time.strftime('%Y-%m-%d')
        my_model_time = date_time.strftime('%H:%M:%S')

    # If there is an offset, shift by offset before proceed
    forecast_timeseries = []
    if 'utcOffset' in my_opts:
        print('Shit by utcOffset:', my_opts['utcOffset'].resolution)
        for item in my_timeseries:
            forecast_timeseries.append(
                [datetime.strptime(item[0], Constants.COMMON_DATE_TIME_FORMAT) + my_opts['utcOffset'], item[1]])

        forecast_timeseries = extractForecastTimeseries(forecast_timeseries, my_model_date, my_model_time, by_day=True)
    else:
        forecast_timeseries = extractForecastTimeseries(my_timeseries, my_model_date, my_model_time, by_day=True)

    # print(forecast_timeseries[:10])
    extracted_timeseries = extractForecastTimeseriesInDays(forecast_timeseries)

    # for ll in extractedTimeseries :
    #     print(ll)

    # Check whether existing station
    force_insert = my_opts.get('forceInsert', False)
    station = my_opts.get('station', '')
    source = my_opts.get('source', 'FLO2D')
    is_station_exists = adapter.get_station({'name': station})
    if is_station_exists is None:
        print('WARNING: Station %s does not exists. Continue with others.' % station)
        return
    # TODO: Create if station does not exists.

    run_name = my_opts.get('run_name', 'Cloud-1')
    less_char_index = run_name.find('<')
    greater_char_index = run_name.find('>')
    if -1 < less_char_index > -1 < greater_char_index:
        start_str = run_name[:less_char_index]
        date_format_str = run_name[less_char_index + 1:greater_char_index]
        end_str = run_name[greater_char_index + 1:]
        try:
            date_str = date_time.strftime(date_format_str)
            run_name = start_str + date_str + end_str
        except ValueError:
            raise ValueError("Incorrect data format " + date_format_str)

    types = [
        'Forecast-0-d',
        'Forecast-1-d-after',
        'Forecast-2-d-after',
        'Forecast-3-d-after',
        'Forecast-4-d-after',
        'Forecast-5-d-after',
        'Forecast-6-d-after',
        'Forecast-7-d-after',
        'Forecast-8-d-after',
        'Forecast-9-d-after',
        'Forecast-10-d-after',
        'Forecast-11-d-after',
        'Forecast-12-d-after',
        'Forecast-13-d-after',
        'Forecast-14-d-after'
    ]
    meta_data = {
        'station': station,
        'variable': 'WaterLevel',
        'unit': 'm',
        'type': types[0],
        'source': source,
        'name': run_name
    }
    for i in range(0, min(len(types), len(extracted_timeseries))):
        meta_data_copy = copy.deepcopy(meta_data)
        meta_data_copy['type'] = types[i]
        event_id = my_adapter.get_event_id(meta_data_copy)
        if event_id is None:
            event_id = my_adapter.create_event_id(meta_data_copy)
            print('HASH SHA256 created: ', event_id)
        else:
            print('HASH SHA256 exists: ', event_id)
            if not force_insert:
                print('Timeseries already exists. User --force to update the existing.\n')
                continue

        # for l in timeseries[:3] + timeseries[-2:] :
        #     print(l)
        row_count = my_adapter.insert_timeseries(event_id, extracted_timeseries[i], force_insert)
        print('%s rows inserted.\n' % row_count)
        # -- END OF SAVE_FORECAST_TIMESERIES


if __name__=="__main__":

    """
    Config.json 
    {
      "HYCHAN_OUT_FILE": "HYCHAN.OUT",
      "TIMEDEP_FILE": "TIMDEP.OUT",
      "WATER_LEVEL_FILE": "water_level.txt",
      "WATER_LEVEL_DIR": "water_level",
      "OUTPUT_DIR": "OUTPUT",
      "RUN_FLO2D_FILE": "RUN_FLO2D.json",
      "UTC_OFFSET": ",+00:00:00",
      "FLO2D_MODEL": "FLO2D",
      
      "model": "WRF",
      "version": "v3",
      
      "unit": "mm",
      "unit_type": "Accumulative",
      
      "variable": "Precipitation",
      
      "host": "127.0.0.1",
      "user": "root",
      "password": "password",
      "db": "curw_fcst",
      "port": 3306
    }

    """
    try:

        # current working directory
        CWD = os.getcwd()

        config = json.loads(open('config.json').read())

        # flo2D related details
        HYCHAN_OUT_FILE = read_attribute_from_config_file('HYCHAN_OUT_FILE', config, True)
        TIMEDEP_FILE = read_attribute_from_config_file('TIMEDEP_FILE', config, True)
        WATER_LEVEL_FILE = read_attribute_from_config_file('WATER_LEVEL_FILE', config, True)
        WATER_LEVEL_DIR = read_attribute_from_config_file('WATER_LEVEL_DIR', config, True)
        OUTPUT_DIR = read_attribute_from_config_file('OUTPUT_DIR', config, True)
        RUN_FLO2D_FILE = read_attribute_from_config_file('RUN_FLO2D_FILE', config, True)
        UTC_OFFSET = read_attribute_from_config_file('UTC_OFFSET', config, True)
        FLO2D_MODEL = read_attribute_from_config_file('FLO2D_MODEL', config, True)

        date = read_attribute_from_config_file('date', config, False)
        time = read_attribute_from_config_file('time', config, False)
        path = read_attribute_from_config_file('path', config, False)
        output_suffix = read_attribute_from_config_file('output_suffix', config, False)
        start_date = read_attribute_from_config_file('start_date', config, False)
        start_time = read_attribute_from_config_file('start_time', config, False)
        flo2d_config = read_attribute_from_config_file('flo2d_config', config, False)
        run_name_default = read_attribute_from_config_file('run_name_default', config, False)
        runName = read_attribute_from_config_file('runName', config, False)
        utc_offset = read_attribute_from_config_file('utc_offset', config, False)
        forceInsert = read_attribute_from_config_file('forceInsert', config, False)

        # source details
        model = read_attribute_from_config_file('model', config, True)
        version = read_attribute_from_config_file('version', config, True)

        # unit details
        unit = read_attribute_from_config_file('unit', config, True)
        unit_type = UnitType.getType(read_attribute_from_config_file('unit_type', config, True))

        # variable details
        variable = read_attribute_from_config_file('variable', config, True)

        # connection params
        host = read_attribute_from_config_file('host', config, True)
        user = read_attribute_from_config_file('user', config, True)
        password = read_attribute_from_config_file('password', config, True)
        db = read_attribute_from_config_file('db', config, True)
        port = read_attribute_from_config_file('port', config, True)

        # pool = get_Pool(host=host, port=port, user=user, password=password, db=db)

        # wrf_v3_stations = get_wrfv3_stations(pool)

        # variable_id = get_variable_id(pool=pool, variable=variable)
        # unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)
        # source_id = get_source_id(pool=pool, model=model, version=version)

        flo2d_source = {"CHANNEL_CELL_MAP": {"179": "Wellawatta Canal-St Peters College", "220": "Dehiwala Canal", "261": "Mutwal Outfall", "387": "Swarna Rd-Wellawatta", "388": "Thummodara", "475": "Babapulle", "545": "Ingurukade Jn", "592": "Torrinton", "616": "Nagalagam Street", "618": "Nagalagam Street River", "660": "OUSL-Narahenpita Rd", "684": "Dematagoda Canal-Orugodawatta", "813": "Kirimandala Mw", "823": "LesliRanagala Mw", "885": "OUSL-Nawala Kirulapana Canal", "912": "Kittampahuwa", "973": "Near SLLRDC", "991": "Kalupalama", "1062": "Yakbedda", "1161": "Kittampahuwa River", "1243": "Vivekarama Mw", "1333": "Wellampitiya", "1420": "Madinnagoda", "1517": "Kotte North Canal", "1528": "Harwad Band", "1625": "Kotiyagoda", "1959": "Koratuwa Rd", "2174": "Weliwala Pond", "2371": "JanakalaKendraya", "2395": "Kelani Mulla Outfall", "2396": "Salalihini-River", "2597": "Old Awissawella Rd", "2693": "Talatel Culvert", "2695": "Wennawatta", "3580": "Ambatale Outfull1", "3673": "Ambatale River", "3919": "Amaragoda", "4192": "Malabe"}, "FLOOD_PLAIN_CELL_MAP": {"24": "Baira Lake Nawam Mw", "153": "Baira Lake Railway", "1838": "Polduwa-Parlimant Rd", "1842": "Abagaha Jn", "2669": "Parlimant Lake Side", "2686": "Aggona", "2866": "Kibulawala 1", "2874": "Rampalawatta"}}

        CHANNEL_CELL_MAP = flo2d_source['CHANNEL_CELL_MAP']

        FLOOD_PLAIN_CELL_MAP = flo2d_source['FLOOD_PLAIN_CELL_MAP']

        ELEMENT_NUMBERS = CHANNEL_CELL_MAP.keys()
        FLOOD_ELEMENT_NUMBERS = FLOOD_PLAIN_CELL_MAP.keys()
        SERIES_LENGTH = 0
        MISSING_VALUE = -999

        appDir = path_join(CWD, date + '_Kelani')
        if path:
            appDir = path_join(CWD, path)

        # Load FLO2D Configuration file for the Model run if available
        FLO2D_CONFIG_FILE = path_join(appDir, RUN_FLO2D_FILE)
        if flo2d_config:
            FLO2D_CONFIG_FILE = path_join(CWD, flo2d_config)
        FLO2D_CONFIG = json.loads('{}')

        # Check FLO2D Config file exists
        if os.path.exists(FLO2D_CONFIG_FILE):
            FLO2D_CONFIG = json.loads(open(FLO2D_CONFIG_FILE).read())

        # Default run for current day
        now = datetime.now()
        if 'MODEL_STATE_DATE' in FLO2D_CONFIG and len(
                FLO2D_CONFIG['MODEL_STATE_DATE']):  # Use FLO2D Config file data, if available
            now = datetime.strptime(FLO2D_CONFIG['MODEL_STATE_DATE'], '%Y-%m-%d')
        if date:
            now = datetime.strptime(date, '%Y-%m-%d')
        date = now.strftime("%Y-%m-%d")

        if 'MODEL_STATE_TIME' in FLO2D_CONFIG and len(
                FLO2D_CONFIG['MODEL_STATE_TIME']):  # Use FLO2D Config file data, if available
            now = datetime.strptime('%s %s' % (date, FLO2D_CONFIG['MODEL_STATE_TIME']), '%Y-%m-%d %H:%M:%S')
        if time:
            now = datetime.strptime('%s %s' % (date, time), '%Y-%m-%d %H:%M:%S')
        time = now.strftime("%H:%M:%S")

        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            start_date = start_date.strftime("%Y-%m-%d")
        elif 'TIMESERIES_START_DATE' in FLO2D_CONFIG and len(
                FLO2D_CONFIG['TIMESERIES_START_DATE']):  # Use FLO2D Config file data, if available
            start_date = datetime.strptime(FLO2D_CONFIG['TIMESERIES_START_DATE'], '%Y-%m-%d')
            start_date = start_date.strftime("%Y-%m-%d")
        else:
            start_date = date

        if start_time:
            start_time = datetime.strptime('%s %s' % (start_date, start_time), '%Y-%m-%d %H:%M:%S')
            start_time = start_time.strftime("%H:%M:%S")
        elif 'TIMESERIES_START_TIME' in FLO2D_CONFIG and len(
                FLO2D_CONFIG['TIMESERIES_START_TIME']):  # Use FLO2D Config file data, if available
            start_time = datetime.strptime('%s %s' % (start_date, FLO2D_CONFIG['TIMESERIES_START_TIME']),
                    '%Y-%m-%d %H:%M:%S')
            start_time = start_time.strftime("%H:%M:%S")
        else:
            start_time = datetime.strptime(start_date, '%Y-%m-%d')  # Time is set to 00:00:00
            start_time = start_time.strftime("%H:%M:%S")

        # Run Name of DB
        if 'RUN_NAME' in FLO2D_CONFIG and len(FLO2D_CONFIG['RUN_NAME']):  # Use FLO2D Config file data, if available
            runName = FLO2D_CONFIG['RUN_NAME']
        if not runName:
            runName = run_name_default

        # UTC Offset
        if 'UTC_OFFSET' in FLO2D_CONFIG and len(FLO2D_CONFIG['UTC_OFFSET']):  # Use FLO2D Config file data, if available
            UTC_OFFSET = FLO2D_CONFIG['UTC_OFFSET']
        if utc_offset:
            UTC_OFFSET = utc_offset
        utcOffset = getUTCOffset(UTC_OFFSET, default=True)

        print('Extract Water Level Result of FLO2D on', date, '@', time, 'with Base time of', start_date, '@',
                start_time)
        print('With UTC Offset of ', str(utcOffset), ' <= ', UTC_OFFSET)

        OUTPUT_DIR_PATH = path_join(CWD, OUTPUT_DIR)
        HYCHAN_OUT_FILE_PATH = path_join(appDir, HYCHAN_OUT_FILE)

        WATER_LEVEL_DIR_PATH = path_join(OUTPUT_DIR_PATH, "%s-%s" % (WATER_LEVEL_DIR, date))
        if 'FLO2D_OUTPUT_SUFFIX' in FLO2D_CONFIG and len(
                FLO2D_CONFIG['FLO2D_OUTPUT_SUFFIX']):  # Use FLO2D Config file data, if available
            WATER_LEVEL_DIR_PATH = path_join(OUTPUT_DIR_PATH,
                    "%s-%s" % (WATER_LEVEL_DIR, FLO2D_CONFIG['FLO2D_OUTPUT_SUFFIX']))
        if output_suffix:
            WATER_LEVEL_DIR_PATH = path_join(OUTPUT_DIR_PATH, "%s-%s" % (WATER_LEVEL_DIR, output_suffix))

        print('Processing FLO2D model on', appDir)

        # Check HYCHAN.OUT file exists
        if not os.path.exists(HYCHAN_OUT_FILE_PATH):
            print('Unable to find file : ', HYCHAN_OUT_FILE_PATH)
            sys.exit()

        # Calculate the size of time series
        bufsize = 65536
        with open(HYCHAN_OUT_FILE_PATH) as infile:
            isWaterLevelLines = False
            isCounting = False
            countSeriesSize = 0  # HACK: When it comes to the end of file, unable to detect end of time series
            while True:
                lines = infile.readlines(bufsize)
                if not lines or SERIES_LENGTH:
                    break
                for line in lines:
                    if line.startswith('CHANNEL HYDROGRAPH FOR ELEMENT NO:', 5):
                        isWaterLevelLines = True
                    elif isWaterLevelLines:
                        cols = line.split()
                        if len(cols) > 0 and cols[0].replace('.', '', 1).isdigit():
                            countSeriesSize += 1
                            isCounting = True
                        elif isWaterLevelLines and isCounting:
                            SERIES_LENGTH = countSeriesSize
                            break

        print('Series Length is :', SERIES_LENGTH)
        bufsize = 65536
        #################################################################
        # Extract Channel Water Level elevations from HYCHAN.OUT file   #
        #################################################################
        print('Extract Channel Water Level Result of FLO2D HYCHAN.OUT on', date, '@', time, 'with Base time of',
                start_date,
                '@', start_time)
        with open(HYCHAN_OUT_FILE_PATH) as infile:
            isWaterLevelLines = False
            isSeriesComplete = False
            waterLevelLines = []
            seriesSize = 0  # HACK: When it comes to the end of file, unable to detect end of time series
            while True:
                lines = infile.readlines(bufsize)
                if not lines:
                    break
                for line in lines:
                    if line.startswith('CHANNEL HYDROGRAPH FOR ELEMENT NO:', 5):
                        seriesSize = 0
                        elementNo = line.split()[5]

                        if elementNo in ELEMENT_NUMBERS:
                            isWaterLevelLines = True
                            waterLevelLines.append(line)
                        else:
                            isWaterLevelLines = False

                    elif isWaterLevelLines:
                        cols = line.split()
                        if len(cols) > 0 and isfloat(cols[0]):
                            seriesSize += 1
                            waterLevelLines.append(line)

                            if seriesSize==SERIES_LENGTH:
                                isSeriesComplete = True

                    if isSeriesComplete:
                        baseTime = datetime.strptime('%s %s' % (start_date, start_time), '%Y-%m-%d %H:%M:%S')
                        timeseries = []
                        elementNo = waterLevelLines[0].split()[5]
                        # print('Extracted Cell No', elementNo, CHANNEL_CELL_MAP[elementNo])
                        for ts in waterLevelLines[1:]:
                            v = ts.split()
                            if len(v) < 1:
                                continue
                            # Get flood level (Elevation)
                            value = v[1]
                            # Get flood depth (Depth)
                            # value = v[2]
                            if not isfloat(value):
                                value = MISSING_VALUE
                                continue  # If value is not present, skip
                            if value=='NaN':
                                continue  # If value is NaN, skip
                            timeStep = float(v[0])
                            currentStepTime = baseTime + timedelta(hours=timeStep)
                            dateAndTime = currentStepTime.strftime("%Y-%m-%d %H:%M:%S")
                            timeseries.append([dateAndTime, value])

                        # Save Forecast values into Database
                        opts = {
                                'forceInsert': forceInsert,
                                'station'    : CHANNEL_CELL_MAP[elementNo],
                                'run_name'   : runName
                                }
                        # print('>>>>>', opts)
                        if utcOffset!=timedelta():
                            opts['utcOffset'] = utcOffset

                        # Push timeseries to database
                        save_forecast_timeseries(adapter, timeseries, date, time, opts)

                        isWaterLevelLines = False
                        isSeriesComplete = False
                        waterLevelLines = []
                # -- END for loop
            # -- END while loop

        #################################################################
        # Extract Flood Plain water elevations from HYCHAN.OUT file       #
        #################################################################
        TIMEDEP_FILE_PATH = path_join(appDir, TIMEDEP_FILE)

        if not os.path.exists(TIMEDEP_FILE_PATH):
            print('Unable to find file : ', TIMEDEP_FILE_PATH)
            sys.exit()

        print('TIMEDEP_FILE_PATH : ', TIMEDEP_FILE_PATH)
        print('Extract Flood Plain Water Level Result of FLO2D on', date, '@', time, 'with Base time of', start_date,
                '@',
                start_time)
        with open(TIMEDEP_FILE_PATH) as infile:
            waterLevelLines = []
            waterLevelSeriesDict = dict.fromkeys(FLOOD_ELEMENT_NUMBERS, [])
            while True:
                lines = infile.readlines(bufsize)
                if not lines:
                    break
                for line in lines:
                    if len(line.split())==1:
                        # continue
                        if len(waterLevelLines) > 0:
                            waterLevels = get_water_level_of_channels(waterLevelLines, FLOOD_ELEMENT_NUMBERS)

                            # Get Time stamp Ref:http://stackoverflow.com/a/13685221/1461060
                            # print('waterLevelLines[0].split() : ', waterLevelLines[0].split())
                            ModelTime = float(waterLevelLines[0].split()[0])
                            baseTime = datetime.strptime('%s %s' % (start_date, start_time), '%Y-%m-%d %H:%M:%S')
                            currentStepTime = baseTime + timedelta(hours=ModelTime)
                            dateAndTime = currentStepTime.strftime("%Y-%m-%d %H:%M:%S")

                            for elementNo in FLOOD_ELEMENT_NUMBERS:
                                tmpTS = waterLevelSeriesDict[elementNo][:]
                                if elementNo in waterLevels:
                                    tmpTS.append([dateAndTime, waterLevels[elementNo]])
                                else:
                                    tmpTS.append([dateAndTime, MISSING_VALUE])
                                waterLevelSeriesDict[elementNo] = tmpTS

                            isWaterLevelLines = False
                            # for l in waterLevelLines :
                            # print(l)
                            waterLevelLines = []
                    waterLevelLines.append(line)

            # print('len(FLOOD_ELEMENT_NUMBERS) : ', len(FLOOD_ELEMENT_NUMBERS))
            for elementNo in FLOOD_ELEMENT_NUMBERS:

                # Save Forecast values into Database
                opts = {
                        'forceInsert': forceInsert,
                        'station'    : FLOOD_PLAIN_CELL_MAP[elementNo],
                        'run_name'   : runName,
                        'source'     : FLO2D_MODEL
                        }
                if utcOffset!=timedelta():
                    opts['utcOffset'] = utcOffset

                # Push timeseries to database
                save_forecast_timeseries(adapter, waterLevelSeriesDict[elementNo], date, time, opts)

        # pool.destroy()

    except Exception as e:
        logger.error('JSON config data loading error.')
        print('JSON config data loading error.')
        traceback.print_exc()
    finally:
        logger.info("Process finished.")
        print("Process finished.")
