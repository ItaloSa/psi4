from __future__ import print_function

import sys
import json
import boto3
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
import requests

devices = {
    "E-82753": "herPCQVR2NKn1mdwFe88",
    "E-82797": "g1nlVmYlyFpoBITdBaId",
    "E-82886": "3c0t32mnBXQflBBZxv2C",
    "E-82890": "YtYcGhIG2J0BwRnCFwzv",
    "E-82893": "nw6GaLKhi5tkKB9l3wsg",
    "E-82900": "jlcZg6o1WpKN6FUahHuT",
    "E-82983": "un7bnEsOPaf8m9x3JXf0",
    "E-A309": "URGTNrxueAKgb6Z7geMk",
    "E-A329": "TVQY0MwXJLVOJpzdq3ek",
    "E-A341": "EWmWPEZUPYPS4hpmgDV8",
    "E-A351": "SOT6YQe3FsrsLWH6NKBg",
    "E-A322": "puJ8GZ0bfuSnD989JE3g",
    "E-A349": "JlFDr16Or66uVbtGjssE",
    "E-A366": "o9JKqoLBW8wwKOKyrpAU",
    "E-A357": "0mz1uFJKK94taExOP5n7",
    "E-A307": "OsHXsSxn3GSnesxaTLU4",
    "E-A301": "O584kL8QumJXisf6f2oz",
    "E-A370": "sknq3gw7n6q4L6e9XAHF",
    "E-A350": "X4eOZah7UfLPfFeAY8wn",
    "E-A328": "dVpE9bqzNv1LGBAFjVMr",
    "ALL-E": "hstxposd8Ibtl6z2jYwr"
}


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


def feed_tb(data):
    try:
        deviceToken = devices[data['cod']]
        allDevToken = devices['ALL-E']
        requests.post(f'http://demo.thingsboard.io/api/v1/{deviceToken}/telemetry', json=data)
        requests.post(f'http://demo.thingsboard.io/api/v1/{allDevToken}/telemetry', json=data)
    except ConnectionError as e:
        print(e)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(
            "Usage: kinesis_spark.py <app-name> <stream-name> <endpoint-url> <region-name>",
            file=sys.stderr)
        sys.exit(-1)

    sqs = boto3.client('sqs')
    sc = SparkContext(appName='kinesis_spark.py')
    ssc = StreamingContext(sc, 1)
    applicationName, streamName, endpointUrl, regionName = sys.argv[1:]
    print("appname is" + applicationName +
          streamName + endpointUrl + regionName)
    stream = KinesisUtils.createStream(
        ssc, applicationName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)

    stream.pprint()

    parsed_with_index = stream.map(
        lambda data: format_data(data))
    parsed_with_index.pprint()

    def handler(rdd):
        stations = rdd.collect()
        print('handler!')
        print('len:')
        print(len(stations))
        for station in stations:
            feed_tb(json.loads(station))

    parsed_with_index.foreachRDD(lambda rdd: handler(rdd))
    parsed_with_index.pprint()

    ssc.start()
    ssc.awaitTermination()
