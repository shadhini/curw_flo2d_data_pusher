
import json
import traceback
import sys
import os
from datetime import datetime, timedelta
import re
import getopt

from db_adapter.logger import logger
from db_adapter.constants import COMMON_DATE_TIME_FORMAT, CURW_FCST_DATABASE, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, \
    CURW_FCST_PORT, CURW_FCST_HOST
from db_adapter.base import get_Pool
from db_adapter.curw_fcst.source import get_source_id, get_source_parameters
from db_adapter.curw_fcst.variable import get_variable_id
from db_adapter.curw_fcst.unit import get_unit_id, UnitType
from db_adapter.curw_fcst.station import get_flo2d_output_stations, StationEnum
from db_adapter.curw_fcst.timeseries import Timeseries

flo2d_stations = { }


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


def check_time_format(time):
    try:
        time = datetime.strptime(time, COMMON_DATE_TIME_FORMAT)

        if time.strftime('%S') != '00':
            print("Seconds should be always 00")
            exit(1)
        if time.strftime('%M') not in ('00', '15', '30', '45'):
            print("Minutes should be always multiples of 15")
            exit(1)

        return True
    except Exception:
        print("Time {} is not in proper format".format(time))
        exit(1)


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

    if utcOffset[0]=="+":  # If timestamp in positive zone, add it to current time
        offset_str = utcOffset[1:].split(':')
        return timedelta(hours=int(offset_str[0]), minutes=int(offset_str[1]))
    if utcOffset[0]=="-":  # If timestamp in negative zone, deduct it from current time
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
    water_levels = { }
    for line in lines[1:]:
        if line=='\n':
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


def extractForecastTimeseries(timeseries, extract_date, extract_time, by_day=False):
    """
    Extracted timeseries upward from given date and time
    E.g. Consider timeseries 2017-09-01 to 2017-09-03
    date: 2017-09-01 and time: 14:00:00 will extract a timeseries which contains
    values that timestamp onwards
    """
    print('LibForecastTimeseries:: extractForecastTimeseries')
    if by_day:
        extract_date_time = datetime.strptime(extract_date, '%Y-%m-%d')
    else:
        extract_date_time = datetime.strptime('%s %s' % (extract_date, extract_time), '%Y-%m-%d %H:%M:%S')

    is_date_time = isinstance(timeseries[0][0], datetime)
    new_timeseries = []
    for i, tt in enumerate(timeseries):
        tt_date_time = tt[0] if is_date_time else datetime.strptime(tt[0], '%Y-%m-%d %H:%M:%S')
        if tt_date_time >= extract_date_time:
            new_timeseries = timeseries[i:]
            break

    return new_timeseries


def save_forecast_timeseries_to_db(pool, timeseries, run_date, run_time, opts, flo2d_stations):
    print('EXTRACTFLO2DWATERLEVEL:: save_forecast_timeseries >>', opts)

    # {
    #         'tms_id'     : '',
    #         'sim_tag'    : '',
    #         'station_id' : '',
    #         'source_id'  : '',
    #         'unit_id'    : '',
    #         'variable_id': ''
    #         }

    # Convert date time with offset
    date_time = datetime.strptime('%s %s' % (run_date, run_time), COMMON_DATE_TIME_FORMAT)
    if 'utcOffset' in opts:
        date_time = date_time + opts['utcOffset']
        run_date = date_time.strftime('%Y-%m-%d')
        run_time = date_time.strftime('%H:%M:%S')

    # If there is an offset, shift by offset before proceed
    forecast_timeseries = []
    if 'utcOffset' in opts:
        print('Shift by utcOffset:', opts['utcOffset'].resolution)
        for item in timeseries:
            forecast_timeseries.append(
                    [datetime.strptime(item[0], COMMON_DATE_TIME_FORMAT) + opts['utcOffset'], item[1]])

        forecast_timeseries = extractForecastTimeseries(timeseries=forecast_timeseries, extract_date=run_date,
                extract_time=run_time)
    else:
        forecast_timeseries = extractForecastTimeseries(timeseries=timeseries, extract_date=run_date,
                extract_time=run_time)

    elementNo = opts.get('elementNo')

    tms_meta = opts.get('tms_meta')

    tms_meta['latitude'] = str(flo2d_stations.get(elementNo)[1])
    tms_meta['longitude'] = str(flo2d_stations.get(elementNo)[2])
    tms_meta['station_id'] = flo2d_stations.get(elementNo)[0]

    try:

        TS = Timeseries(pool=pool)

        tms_id = TS.get_timeseries_id_if_exists(meta_data=tms_meta)

        fgt = (datetime.now() + timedelta(hours=5, minutes=30)).strftime(COMMON_DATE_TIME_FORMAT)

        if tms_id is None:
            tms_id = TS.generate_timeseries_id(meta_data=tms_meta)
            tms_meta['tms_id'] = tms_id
            TS.insert_run(run_meta=tms_meta)
            TS.update_start_date(id_=tms_id, start_date=fgt)

        TS.insert_data(timeseries=forecast_timeseries, tms_id=tms_id, fgt=fgt, upsert=True)
        TS.update_latest_fgt(id_=tms_id, fgt=fgt)

    except Exception:
        logger.error("Exception occurred while pushing data to the curw_fcst database")
        traceback.print_exc()


def usage():
    usageText = """
    Usage: .\extract_water_level_manually.py [-m flo2d_XXX] [-s "YYYY-MM-DD HH:MM:SS"] [-r "YYYY-MM-DD HH:MM:SS"] 
    [-d "D:\inflow\flo2d_hourly\output"]

    -h  --help          Show usage
    -m  --model         FLO2D model (e.g. flo2d_250, flo2d_150).
    -s  --ts_start_time Timeseries start time (e.g: "2019-06-05 23:00:00").
    -r  --run_time      Run time (e.g: "2019-06-05 23:00:00").
    -d  --dir           Output directory (e.g. "D:\inflow\flo2d_hourly\output")
    """
    print(usageText)


if __name__ == "__main__":

    """
    Config.json 
    {
      "HYCHAN_OUT_FILE": "HYCHAN.OUT",
      "TIMDEP_FILE": "TIMDEP.OUT",
      "output_dir": "/home/shadhini/dev/repos/shadhini/flo2d_data_pusher/2019-05-24_Kelani",
    
      "run_date": "2019-05-24",
      "run_time": "00:00:00",
      "ts_start_date": "2019-05-24",
      "ts_start_time": "00:00:00",
      "utc_offset": "",
    
      "sim_tag": "manual_run",
    
      "model": "FLO2D",
      "version": "250",
    
      "unit": "m",
      "unit_type": "Instantaneous",
    
      "variable": "WaterLevel"
    }

    """
    try:

        print("started extracting flo2d output")
        in_ts_start_time = None
        in_run_time = None
        flo2d_model = None
        output_dir = None

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:m:s:r:d:",
                                       ["help", "model=", "ts_start_time=", "run_time=", "dir="])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-m", "--model"):
                flo2d_model = arg.strip()
            elif opt in ("-s", "--ts_start_time"):
                in_ts_start_time = arg.strip()
            elif opt in ("-r", "--run_time"):
                in_run_time = arg.strip()
            elif opt in ("-d", "--dir"):
                output_dir = arg.strip()

        config = json.loads(open('config.json').read())

        # flo2D related details
        HYCHAN_OUT_FILE = read_attribute_from_config_file('HYCHAN_OUT_FILE', config, True)
        TIMDEP_FILE = read_attribute_from_config_file('TIMDEP_FILE', config, True)

        if in_ts_start_time is None:
            print("Please specify the time series start time.")
            usage()
            exit(1)
        if in_run_time is None:
            print("Please specify run time.")
            usage()
            exit(1)
        if flo2d_model is None:
            print("Please specify flo2d model.")
            usage()
            exit(1)
        if output_dir is None:
            print("Please specify flo2d output directory.")
            usage()
            exit(1)

        if not os.path.isdir(output_dir):
            print("Given output directory doesn't exist")
            exit(1)
        if flo2d_model not in ("flo2d_250", "flo2d_150"):
            print("Flo2d model should be either \"flo2d_250\" or \"flo2d_150\"")
            exit(1)

        check_time_format(in_ts_start_time)
        check_time_format(in_run_time)

        output_dir = output_dir

        run_date = in_run_time.striftime("%Y-%m-%d")
        run_time = in_run_time.striftime("%H:%M:%S")
        ts_start_date = in_ts_start_time.striftime("%Y-%m-%d")
        ts_start_time = in_ts_start_time.striftime("%H:%M:%S")

        utc_offset = read_attribute_from_config_file('utc_offset', config, False)
        if utc_offset is None:
            utc_offset = ''

        # sim tag
        sim_tag = read_attribute_from_config_file('sim_tag', config, True)

        # source details
        model = read_attribute_from_config_file('model', config, True)
        version = flo2d_model.split("_")[1]

        # unit details
        unit = read_attribute_from_config_file('unit', config, True)
        unit_type = UnitType.getType(read_attribute_from_config_file('unit_type', config, True))

        # variable details
        variable = read_attribute_from_config_file('variable', config, True)

        hychan_out_file_path = os.path.join(output_dir, HYCHAN_OUT_FILE)
        timdep_file_path = os.path.join(output_dir, TIMDEP_FILE)

        pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT, db=CURW_FCST_DATABASE, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD)

        flo2d_model_name = '{}_{}'.format(model, version)

        flo2d_source = json.loads(get_source_parameters(pool=pool, model=model, version=version))
        flo2d_stations = get_flo2d_output_stations(pool=pool, flo2d_model=StationEnum.getType(flo2d_model_name))

        source_id = get_source_id(pool=pool, model=model, version=version)

        variable_id = get_variable_id(pool=pool, variable=variable)

        unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)

        tms_meta = {
                'sim_tag'    : sim_tag,
                'model'      : model,
                'version'    : version,
                'variable'   : variable,
                'unit'       : unit,
                'unit_type'  : unit_type.value,
                'source_id'  : source_id,
                'variable_id': variable_id,
                'unit_id'    : unit_id
                }

        CHANNEL_CELL_MAP = flo2d_source["CHANNEL_CELL_MAP"]

        FLOOD_PLAIN_CELL_MAP = flo2d_source["FLOOD_PLAIN_CELL_MAP"]

        ELEMENT_NUMBERS = CHANNEL_CELL_MAP.keys()
        FLOOD_ELEMENT_NUMBERS = FLOOD_PLAIN_CELL_MAP.keys()
        SERIES_LENGTH = 0
        MISSING_VALUE = -999

        utcOffset = getUTCOffset(utc_offset, default=True)

        print('Extract Water Level Result of FLO2D on', run_date, '@', run_time, 'with Base time of', ts_start_date,
                '@', ts_start_time)

        # Check HYCHAN.OUT file exists
        if not os.path.exists(hychan_out_file_path):
            print('Unable to find file : ', hychan_out_file_path)
            traceback.print_exc()

        #####################################
        # Calculate the size of time series #
        #####################################
        bufsize = 65536
        with open(hychan_out_file_path) as infile:
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
        print('Extract Channel Water Level Result of FLO2D (HYCHAN.OUT) on', run_date, '@', run_time,
                'with Base time of',
                ts_start_date, '@', ts_start_time)
        with open(hychan_out_file_path) as infile:
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
                        baseTime = datetime.strptime('%s %s' % (ts_start_date, ts_start_time), '%Y-%m-%d %H:%M:%S')
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
                                'elementNo': elementNo,
                                'tms_meta' : tms_meta
                                }
                        # print('>>>>>', opts)
                        if utcOffset!=timedelta():
                            opts['utcOffset'] = utcOffset

                        # Push timeseries to database
                        save_forecast_timeseries_to_db(pool=pool, timeseries=timeseries,
                                run_date=run_date, run_time=run_time, opts=opts, flo2d_stations=flo2d_stations)

                        isWaterLevelLines = False
                        isSeriesComplete = False
                        waterLevelLines = []
                # -- END for loop
            # -- END while loop

        #################################################################
        # Extract Flood Plain water elevations from TIMEDEP.OUT file    #
        #################################################################

        if not os.path.exists(timdep_file_path):
            print('Unable to find file : ', timdep_file_path)
            traceback.print_exc()

        print('Extract Flood Plain Water Level Result of FLO2D (TIMEDEP.OUT) on', run_date, '@', run_time,
                'with Base time of', ts_start_date,
                '@', ts_start_time)

        with open(timdep_file_path) as infile:
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
                            baseTime = datetime.strptime('%s %s' % (ts_start_date, ts_start_time), '%Y-%m-%d %H:%M:%S')
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
                        'elementNo': elementNo,
                        'tms_meta' : tms_meta
                        }
                if utcOffset!=timedelta():
                    opts['utcOffset'] = utcOffset

                # Push timeseries to database
                save_forecast_timeseries_to_db(pool=pool, timeseries=waterLevelSeriesDict[elementNo],
                        run_date=run_date, run_time=run_time, opts=opts, flo2d_stations=flo2d_stations)

    except Exception as e:
        logger.error('JSON config data loading error.')
        print('JSON config data loading error.')
        traceback.print_exc()
    finally:
        logger.info("Process finished.")
        print("Process finished.")