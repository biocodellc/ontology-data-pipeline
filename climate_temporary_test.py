# -*- coding: utf-8 -*-
import os
from climate import climate

CLIMATE_DATA_DIR = os.path.join(os.path.dirname(__file__), "./data/climate_data/")

climate_data = climate.ClimateData(CLIMATE_DATA_DIR)

months =[i for i in range(12)]

print ('here are 2016 monthly mean temperatures for junction city oregon...')
print ('these values do not appear to be correct')
for month in months:
    val = climate_data.get_data(2015, month+1, float(44.22), float(-123.345))
    print(val)

