import os
from builtins import print
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from urllib.parse import urlparse, parse_qs
# from raincelldat.gen_raincell import create_hybrid_raincell
from curw_sim.gen_raincell_curw_sim import create_sim_hybrid_raincell
from inflowdat.get_inflow import create_inflow
from outflowdat.gen_outflow_old import create_outflow_old
from outflowdat.gen_outflow import create_outflow
from flo2d.run_model import execute_flo2d_250m, flo2d_model_completed
from waterlevel.upload_waterlevel import upload_waterlevels_curw
from extract.extract_water_level_hourly_run import upload_waterlevels
from extract.extract_discharge_hourly_run import upload_discharges
from os.path import join as pjoin
from datetime import datetime, timedelta

HOST_ADDRESS = '10.138.0.4'
# HOST_ADDRESS = '0.0.0.0'
HOST_PORT = 8088


def set_daily_dir(run_date, run_time):
    start_datetime = datetime.strptime('%s %s' % (run_date, run_time), '%Y-%m-%d %H:%M:%S')
    run_time = start_datetime.strftime('%H-%M-%S')
    dir_path = pjoin(os.getcwd(), 'output', run_date, run_time)
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
        except OSError as e:
            print(str(e))
    print('set_daily_dir|dir_path : ', dir_path)
    return dir_path


class StoreHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.timeout = 2100
        print('Handle GET request...')
        if self.path.startswith('/create-raincell'):
            os.chdir(r"D:\flo2d_hourly")
            print('create-raincell')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                [forward] = query_components["forward"]
                [backward] = query_components["backward"]
                print('[run_date, run_time] : ', [run_date, run_time])
                dir_path = set_daily_dir(run_date, run_time)
                create_hybrid_raincell(dir_path, run_date, run_time, forward, backward)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-sim-raincell'):
            os.chdir(r"D:\flo2d_hourly")
            print('create-sim-raincell')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                [forward] = query_components["forward"]
                [backward] = query_components["backward"]
                print('[run_date, run_time] : ', [run_date, run_time])
                dir_path = set_daily_dir(run_date, run_time)
                create_sim_hybrid_raincell(dir_path, run_date, run_time, forward, backward,
                                           res_mins=5, flo2d_model='flo2d_250',
                                           calc_method='MME')
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-inflow'):
            os.chdir(r"D:\flo2d_hourly")
            print('create-inflow')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                print('[run_date, run_time] : ', [run_date, run_time])
                dir_path = set_daily_dir(run_date, run_time)
                backward = '2'
                forward = '3'
                duration_days = (int(backward), int(forward))
                ts_start_date = datetime.strptime(run_date, '%Y-%m-%d') - timedelta(days=duration_days[0])
                ts_end_date = datetime.strptime(run_date, '%Y-%m-%d') + timedelta(days=duration_days[1])
                ts_end_date = ts_end_date.strftime('%Y-%m-%d')
                ts_start_date = ts_start_date.strftime('%Y-%m-%d')
                ts_start_time = '00:00:00'
                ts_start = '{} {}'.format(ts_start_date, ts_start_time)
                ts_end = '{} {}'.format(ts_end_date, ts_start_time)
                print('create_inflow-[ts_start, ts_end]', [ts_start, ts_end])
                create_inflow(dir_path, ts_start, ts_end)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-outflow'):
            os.chdir(r"D:\flo2d_hourly")
            print('create-outflow')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                [forward] = query_components["forward"]
                [backward] = query_components["backward"]
                print('[run_date, run_time] : ', [run_date, run_time])
                dir_path = set_daily_dir(run_date, run_time)
                duration_days = (int(backward), int(forward))
                ts_start_date = datetime.strptime(run_date, '%Y-%m-%d') - timedelta(days=duration_days[0])
                ts_end_date = datetime.strptime(run_date, '%Y-%m-%d') + timedelta(days=duration_days[1])
                ts_end_date = ts_end_date.strftime('%Y-%m-%d')
                ts_start_date = ts_start_date.strftime('%Y-%m-%d')
                ts_start_time = '00:00:00'
                ts_start = '{} {}'.format(ts_start_date, ts_start_time)
                ts_end = '{} {}'.format(ts_end_date, ts_start_time)
                print('create_outflow-[ts_start, ts_end]', [ts_start, ts_end])
                create_outflow(dir_path, ts_start, ts_end)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/create-outflow-old'):
            os.chdir(r"D:\flo2d_hourly")
            print('create-outflow')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                [forward] = query_components["forward"]
                [backward] = query_components["backward"]
                print('[run_date, run_time] : ', [run_date, run_time])
                dir_path = set_daily_dir(run_date, run_time)
                duration_days = (int(backward), int(forward))
                ts_start_date = datetime.strptime(run_date, '%Y-%m-%d') - timedelta(days=duration_days[0])
                ts_end_date = datetime.strptime(run_date, '%Y-%m-%d') + timedelta(days=duration_days[1] + 1)
                ts_end_date = ts_end_date.strftime('%Y-%m-%d')
                ts_start_date = ts_start_date.strftime('%Y-%m-%d')
                ts_start_time = '00:00:00'
                ts_start = '{} {}'.format(ts_start_date, ts_start_time)
                ts_end = '{} {}'.format(ts_end_date, ts_start_time)
                print('create_outflow-[ts_start, ts_end]', [ts_start, ts_end])
                create_outflow_old(dir_path, ts_start, ts_end)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/run-flo2d'):
            os.chdir(r"D:\flo2d_hourly")
            print('run-flo2d')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                print('[run_date, run_time] : ', [run_date, run_time])
                dir_path = set_daily_dir(run_date, run_time)
                execute_flo2d_250m(dir_path, run_date, run_time)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/flo2d-completed'):
            os.chdir(r"D:\flo2d_hourly")
            print('flo2d-completed')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                dir_path = set_daily_dir(run_date, run_time)
                try:
                    flo2d_model_completed(dir_path, run_date, run_time)
                except Exception as ex:
                    print('flo2d_model_completed|Exception : ', str(ex))
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/extract-data'):
            os.chdir(r"D:\flo2d_hourly")
            print('extract-data')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                dir_path = set_daily_dir(run_date, run_time)
                backward = '2'
                forward = '3'
                duration_days = (int(backward), int(forward))
                ts_start_date = datetime.strptime(run_date, '%Y-%m-%d') - timedelta(days=duration_days[0])
                ts_start_date = ts_start_date.strftime('%Y-%m-%d')
                ts_start_time = '00:00:00'
                # upload_waterlevels_curw(dir_path, ts_start_date, ts_start_time)
                upload_waterlevels(dir_path, ts_start_date, ts_start_time, run_date, run_time)
                # upload discharges to curw_fcst database
                upload_discharges(dir_path, ts_start_date, ts_start_time, run_date, run_time)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))

        if self.path.startswith('/extract-curw'):
            os.chdir(r"D:\flo2d_hourly")
            print('extract-data')
            response = {}
            try:
                query_components = parse_qs(urlparse(self.path).query)
                print('query_components : ', query_components)
                [run_date] = query_components["run_date"]
                [run_time] = query_components["run_time"]
                dir_path = set_daily_dir(run_date, run_time)
                backward = '2'
                forward = '3'
                duration_days = (int(backward), int(forward))
                ts_start_date = datetime.strptime(run_date, '%Y-%m-%d') - timedelta(days=duration_days[0])
                ts_start_date = ts_start_date.strftime('%Y-%m-%d')
                run_date = datetime.strptime(run_date, '%Y-%m-%d') + timedelta(days=1)
                run_date = run_date.strftime('%Y-%m-%d')
                ts_start_time = '00:00:00'
                print('upload_waterlevels_curw|[ts_start_date, ts_start_time, run_date, run_time] : ', [ts_start_date,
                                                                                                        ts_start_time,
                                                                                                        run_date,
                                                                                                        run_time])
                upload_waterlevels_curw(dir_path, ts_start_date, ts_start_time, run_date, run_time)
                response = {'response': 'success'}
            except Exception as e:
                print(str(e))
            reply = json.dumps(response)
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()
            self.wfile.write(str.encode(reply))


if __name__ == '__main__':
    try:
        print('starting server...')
        server_address = (HOST_ADDRESS, HOST_PORT)
        httpd = HTTPServer(server_address, StoreHandler)
        print('running server...')
        httpd.serve_forever()
    except Exception as e:
        print(str(e))


