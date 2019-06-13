import traceback
import json

from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import add_station, StationEnum
from db_adapter.constants import CURW_FCST_HOST, CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_PORT, CURW_FCST_DATABASE
from db_adapter.curw_sim.constants import FLO2D_250, FLO2D_150

from db_adapter.csv_utils import read_csv

from logger import logger


if __name__=="__main__":

    try:

        #####################################################
        # Initialize parameters for FLO2D_250 and FLO2D_150 #
        #####################################################

        # source details
        FLO2D_250_params = json.loads(open('flo2d_250.json').read())
        FLO2D_150_params = json.loads(open('flo2d_150.json').read())
        FLO2D_model = 'FLO2D'
        FLO2D_250_version = '250'
        FLO2D_150_version = '150'

        # unit details
        unit = 'm'
        unit_type = UnitType.getType('Instantaneous')

        # variable details
        variable = 'WaterLevel'

        # station details
        flo2d_250_grids = read_csv('flo2d_250m.csv')
        flo2d_150_grids = read_csv('flo2d_150m.csv')

        pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                db=CURW_FCST_DATABASE)

        # ########
        # # test
        # ########
        #
        # USERNAME = "root"
        # PASSWORD = "password"
        # HOST = "127.0.0.1"
        # PORT = 3306
        # DATABASE = "test_schema"
        #
        # pool = get_Pool(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE)

        add_source(pool=pool, model=FLO2D_model, version=FLO2D_250_version, parameters=FLO2D_250_params)
        add_source(pool=pool, model=FLO2D_model, version=FLO2D_150_version, parameters=FLO2D_150_params)
        add_variable(pool=pool, variable=variable)
        add_unit(pool=pool, unit=unit, unit_type=unit_type)

        # add flo2d 250 output stations

        channel_cell_map_250 = FLO2D_250_params.get('CHANNEL_CELL_MAP')

        for channel_cell_map_250_key in channel_cell_map_250.keys():
            add_station(pool=pool, name="{}_{}".format(channel_cell_map_250_key, channel_cell_map_250.get(channel_cell_map_250_key)),
                    latitude="%.6f" % float(flo2d_250_grids[int(channel_cell_map_250_key)-1][2]),
                    longitude="%.6f" % float(flo2d_250_grids[int(channel_cell_map_250_key)-1][1]),
                    station_type=StationEnum.FLO2D_250, description="{}_channel_cell_map_element".format(FLO2D_250))

        flood_plain_cell_map_250 = FLO2D_250_params.get('FLOOD_PLAIN_CELL_MAP')

        for flood_plain_cell_map_250_key in flood_plain_cell_map_250.keys():
            add_station(pool=pool, name="{}_{}".format(flood_plain_cell_map_250_key, flood_plain_cell_map_250.get(flood_plain_cell_map_250_key)),
                    latitude="%.6f" % float(flo2d_250_grids[int(flood_plain_cell_map_250_key)-1][2]),
                    longitude="%.6f" % float(flo2d_250_grids[int(flood_plain_cell_map_250_key)-1][1]),
                    station_type=StationEnum.FLO2D_250, description="{}_flood_plain_cell_map_element".format(FLO2D_250))

        # add flo2d 150 output stations

        channel_cell_map_150 = FLO2D_150_params.get('CHANNEL_CELL_MAP')

        for channel_cell_map_150_key in channel_cell_map_150.keys():
            add_station(pool=pool, name="{}_{}".format(channel_cell_map_150_key, channel_cell_map_150.get(channel_cell_map_150_key)),
                    latitude="%.6f" % float(flo2d_150_grids[int(channel_cell_map_150_key) - 1][2]),
                    longitude="%.6f" % float(flo2d_150_grids[int(channel_cell_map_150_key) - 1][1]),
                    station_type=StationEnum.FLO2D_150, description="{}_channel_cell_map_element".format(FLO2D_150))

        flood_plain_cell_map_150 = FLO2D_150_params.get('FLOOD_PLAIN_CELL_MAP')

        for flood_plain_cell_map_150_key in flood_plain_cell_map_150.keys():
            add_station(pool=pool,
                    name="{}_{}".format(flood_plain_cell_map_150_key, flood_plain_cell_map_150.get(flood_plain_cell_map_150_key)),
                    latitude="%.6f" % float(flo2d_150_grids[int(flood_plain_cell_map_150_key) - 1][2]),
                    longitude="%.6f" % float(flo2d_150_grids[int(flood_plain_cell_map_150_key) - 1][1]),
                    station_type=StationEnum.FLO2D_150, description="{}_flood_plain_cell_map_element".format(FLO2D_150))

        destroy_Pool(pool=pool)

    except Exception:
        logger.info("Initialization process failed.")
        traceback.print_exc()
    finally:
        logger.info("Initialization process finished.")
