#!/usr/bin/env bash

# Print execution date time
echo `date`

#echo "Changing into ~/flo2d_data_pusher"
#cd /home/uwcc-admin/flo2d_data_pusher
#echo "Inside `pwd`"

cd /home/shadhini/Documents/CUrW/Flo2D

# If no venv (python3 virtual environment) exists, then create one.
if [ ! -d "venv" ]
then
    echo "Creating venv python3 virtual environment."
    virtualenv -p python3 venv
fi

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

# Install dependencies using pip.
if [ ! -f "flo2d_data_pusher.log" ]
then
    echo "Installing numpy"
    pip install numpy
    echo "Installing netCDF4"
    pip install netCDF4
    echo "Installing cftime"
    pip install cftime
    echo "Installing PyMySQL"
    pip install PyMySQL
    echo "Installing PyYAML"
    pip install PyYAML
    echo "Installing datalayer"
    pip install git+https://github.com/shadhini/curw_db_adapter.git -U
fi

# Push WRFv3 data into the database
echo "Running data_pusher.py. Logs Available in wrfv3_data_pusher.log file."
python extract_water_level.py >> flo2d_data_pusher.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
