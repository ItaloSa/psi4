import sys
import json
import boto3
import os
import base64

client = boto3.client('kinesis')


def celsius_to_fahr(t):
    return (t * 9/5) + 32


def fahr_to_celsius(t):
    return (t - 32) * 5/9


def calc_hi(T, RH):

    HI_S = (1.1 * T) - 10.3 + (0.047 * RH)

    if HI_S < 80:
        HI = HI_S
        return HI

    HI_S = -42.379 + (2.04901523 * T) + (10.14333127 * RH) + (-0.22475541 * T * RH) + \
        (-6.83783e-03 * T**2) + (-5.481717e-02 * RH**2) + (1.22874e-03 * T**2 * RH) + \
        (8.5282e-04 * T * RH**2) + (-1.99e-06 * T**2 * RH**2)

    if (80 <= T and T <= 112) and RH <= 13:
        HI = HI_S - ((3.25 - (0.25 * RH)) * ((17 - abs(T - 95))/17)**0.5)
        return HI

    if (80 <= T and T <= 87) and RH > 85:
        HI = HI_S + (0.02 * (RH - 85) * (87 * T))
        return HI

    HI = HI_S
    return HI


def get_hi_state(hi):
    hi = float(hi)
    if hi <= 27:
        return "NORMAL"
    elif hi >= 27.1 and hi <= 32:
        return "CAUTELA"
    elif hi >= 32.1 and hi <= 41:
        return "CAUTELA EXTREMA"
    elif hi >= 41.1 and hi <= 54:
        return "PERIGO"
    elif hi > 54:
        return "PERIGO EXTREMO"
    else:
        return "N/A"


def format_data(data):
    station_infos = json.loads(data)

    temp_med = station_infos["medTemp"]
    umid_med = station_infos["medhumd"]

    temp_min = station_infos["minTemp"]
    temp_max = station_infos["maxTemp"]

    if temp_med == None:
        if None not in (temp_min, temp_max):
            calc_temp_med = (float(temp_max) + float(temp_min)) / 2
            temp_med = float("{:.2f}".format(calc_temp_med))
            station_infos["medTemp"] = temp_med

    if None not in (temp_med, umid_med):
        T = celsius_to_fahr(float(temp_med))
        RH = float(umid_med)
        index_in_fahr = calc_hi(T, RH)
        station_infos["hi"] = float(
            "{:.2f}".format(fahr_to_celsius(index_in_fahr)))
        station_infos["hiState"] = get_hi_state(
            station_infos["hi"])

    else:
        station_infos["hi"] = "N/A"
        station_infos["hiState"] = "N/A"

    for (key, value) in station_infos.items():
        if value == None:
            station_infos[key] = "N/A"

    return json.dumps(station_infos)


def post_to_kinesis(data):
    return client.put_record(
        StreamName=os.getenv('OUTP_STREAM'),
        Data=data,
        PartitionKey='1'
    )


def main(event, context):
    for record in event['Records']:
        data = base64.b64decode(record['kinesis']['data'])
        data = data.decode('utf-8')
        process_result = format_data(data)
        post_to_kinesis(process_result.encode('utf-8'))
        print(process_result)
