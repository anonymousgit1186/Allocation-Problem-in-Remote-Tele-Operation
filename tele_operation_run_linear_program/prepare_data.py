import collections
import os
import random
import numpy as np
import pandas as pd
from xml.etree import ElementTree
from scipy import stats as stats
from scipy.stats import norm
import json

COL_SUFFIX_TIME = "_time_participant"
COL_SUFFIX_QUALITY = "_quality"
COL_PARTICIPANT_ID = 'participantId'
scenarios_dict = {"scenario0": "park_scenario_a", "scenario1": "turn_left_scenario_a",
                  "scenario2": "slow_vehicle_scenario_a", "scenario3": "foreign_object_scenario_a",
                  "scenario4": "extreme_weather_scenario_a"}

MIN_TIME = 7 / 5
MAX_TIME = 288 / 5
alpha = [0.7, 0.4, 0.4, 0, 0.2, 0.6]
TICKS_IN_HOUR = 720


def create_scenarios_no_dup():
    path_to_scenarios_misssions = 'extensive_scenarios_xml_to_missions/'
    max_t = 0
    for filename in os.listdir(path_to_scenarios_misssions):
        if not os.path.isfile(os.path.join(path_to_scenarios_misssions, filename)):
            continue
        df = pd.read_csv(os.path.join(path_to_scenarios_misssions, filename), index_col=False)
        df = df.sort_values(by=['arrivalTime'])
        dup = {item: count for item, count in collections.Counter(df['arrivalTime'].tolist()).items() if count > 1}
        arrivalTimes = df['arrivalTime'].tolist()
        for i, t in enumerate(arrivalTimes):
            if t in dup:
                for j in range(i + 1, len(arrivalTimes)):
                    arrivalTimes[j] += 1
                dup = {item: count for item, count in collections.Counter(arrivalTimes).items() if count > 1}

        if max(arrivalTimes) > max_t:
            max_t = max(arrivalTimes)

        df = df.assign(arrivalTime=arrivalTimes)
        df.to_csv(path_to_scenarios_misssions + '/no_dup/no_dup_' + filename)
    return max_t


def create_pjt(T, C):
    return create_pjt_5_hours(T, C)


def create_pjt_24_hors(T, C):
    pjt_stats = pd.read_csv('data/pjt_stats_3_states.csv')
    pjt = {}
    for t in range(T):
        hour = t // TICKS_IN_HOUR
        arrival_prob = pjt_stats[pjt_stats['hour'] == hour]['probability']._values[0]
        pjt[t] = {}
        for j, scenario in enumerate(scenarios_dict.values()):
            scenario_precent = pjt_stats[pjt_stats['hour'] == hour][scenario]._values[0]
            pjt[t][j] = arrival_prob * scenario_precent  # uniform distribution of 5 types and none option
    for t in range(T, T + C):
        pjt[t] = {}
        for j, scenario in enumerate(scenarios_dict.values()):
            pjt[t][j] = 0.0000001
    return pjt


def create_pjt_5_hours(T=TICKS_IN_HOUR * 5, C=30):
    pjt_stats = pd.read_csv('data/pjt_stats_3_states_5_hours.csv')
    pjt = {}
    for t in range(T):
        hour = (t // TICKS_IN_HOUR) + 17
        arrival_prob = pjt_stats[pjt_stats['hour'] == hour]['probability']._values[0]
        pjt[t] = {}
        for j, scenario in enumerate(scenarios_dict.values()):
            scenario_precent = pjt_stats[pjt_stats['hour'] == hour][scenario]._values[0]
            pjt[t][j] = arrival_prob * scenario_precent  # uniform distribution of 5 types and none option
    for t in range(T, T + C):
        pjt[t] = {}
        for j, scenario in enumerate(scenarios_dict.values()):
            pjt[t][j] = 0.0000001
    return pjt


def get_workers():
    print(os.getcwd())
    xmlRoot = ElementTree.parse('scenario_1.xml').getroot()

    workers_ID = []
    for workers in xmlRoot.findall('workers'):
        for i, worker in enumerate(workers.findall('worker')):
            workers_ID.append(worker.attrib['id'])
    return workers_ID


def normalizeTime(x, mean=None, std=None):
    if x == 0:
        return 0

    x = np.log(x)

    return (x - np.log(MIN_TIME)) / (np.log(MAX_TIME) - np.log(MIN_TIME))


def create_W(workers, C):
    return create_W_with_WT(workers, C)


def create_W_with_WT(workers, C):
    W = {}
    exp_data = pd.read_csv('data/dataExcelCSV_analized.csv')

    for i, workerID in enumerate(workers):
        W[i] = {}
        row = exp_data.loc[exp_data[COL_PARTICIPANT_ID] == workerID]

        for j, scenario_type in enumerate(scenarios_dict.values()):
            W[i][j] = {}
            real_time = int(row[scenario_type + COL_SUFFIX_TIME].item())
            mu = real_time / 5
            std = 2
            lower = 7 / 5
            random_real_time = \
                stats.truncnorm((lower - mu) / std, (np.inf - mu) / std, loc=mu, scale=std).rvs(1)[0]
            for wt in range(C):
                W[i][j][wt] = 1- ( np.log(random_real_time + wt) / np.log(MAX_TIME + C) * alpha[0])
    return W


def create_W_expect_times_reg(workers):
    W = {}
    exp_data = pd.read_csv('data/dataExcelCSV_analized.csv')

    for i, workerID in enumerate(workers):
        W[i] = {}
        row = exp_data.loc[exp_data[COL_PARTICIPANT_ID] == workerID]

        for j, scenario_type in enumerate(scenarios_dict.values()):
            real_time = int(row[scenario_type + COL_SUFFIX_TIME].item())
            mu = real_time / 5
            std = 2
            lower = 7 / 5
            random_real_time = \
                stats.truncnorm((lower - mu) / std, (np.inf - mu) / std, loc=mu, scale=std).rvs(1)[0]

            W[i][j] = (-1 * normalizeTime(random_real_time) * alpha[0]) + 1
    return W


def create_W_real_times_1_std(workers):
    W = {}
    exp_data = pd.read_csv('data/dataExcelCSV_analized.csv')

    for i, workerID in enumerate(workers):
        W[i] = {}
        row = exp_data.loc[exp_data[COL_PARTICIPANT_ID] == workerID]

        for j, scenario_type in enumerate(scenarios_dict.values()):
            real_time = int(row[scenario_type + COL_SUFFIX_TIME].item())
            mu = real_time / 5
            std = 1
            lower = 7 / 5
            random_real_time = \
                stats.truncnorm((lower - mu) / std, (np.inf - mu) / std, loc=mu, scale=std).rvs(1)[0]

            W[i][j] = (-1 * normalizeTime(random_real_time) * alpha[0]) + 1
    return W

def create_C_lambda(workers):
    return create_C_lambda_real_times_reg(workers)


def create_C_lambda_real_times_reg(workers):
    C_lambda = dict()
    exp_data = pd.read_csv('data/dataExcelCSV_analized.csv')

    for i, workerID in enumerate(workers):
        C_lambda[i] = {}
        row = exp_data.loc[exp_data[COL_PARTICIPANT_ID] == workerID]

        for j, scenario_type in enumerate(scenarios_dict.values()):
            real_time = int(row[scenario_type + COL_SUFFIX_TIME].item())
            mu = real_time / 5
            std = 2
            C_lambda[i][j] = str(mu) + ';' + str(std)
    return C_lambda


def create_C_lambda_1_std(workers):
    C_lambda = dict()
    exp_data = pd.read_csv('data/dataExcelCSV_analized.csv')

    for i, workerID in enumerate(workers):
        C_lambda[i] = {}
        row = exp_data.loc[exp_data[COL_PARTICIPANT_ID] == workerID]

        for j, scenario_type in enumerate(scenarios_dict.values()):
            real_time = int(row[scenario_type + COL_SUFFIX_TIME].item())
            mu = real_time / 5
            std = 1
            C_lambda[i][j] = str(mu) + ';' + str(std)
    return C_lambda


def create_C_lambda_0_std(workers):
    C_lambda = dict()
    exp_data = pd.read_csv('data/dataExcelCSV_analized.csv')

    for i, workerID in enumerate(workers):
        C_lambda[i] = {}
        row = exp_data.loc[exp_data[COL_PARTICIPANT_ID] == workerID]

        for j, scenario_type in enumerate(scenarios_dict.values()):
            real_time = int(row[scenario_type + COL_SUFFIX_TIME].item())
            mu = real_time / 5
            std = 0
            C_lambda[i][j] = str(mu) + ';' + str(std)
    return C_lambda


def create_C_lambda_probs(LHS, RHS, T, C, C_lambda_dict):
    return create_C_lambda_probs_reg(LHS, RHS, T, C, C_lambda_dict)


def create_C_lambda_probs_reg(LHS, RHS, T, C, C_lambda_dict):
    C_lambda_probs = {}
    arr = np.asarray(range(T + C))
    for i in range(LHS):
        C_lambda_probs[i] = {}
        for j in range(RHS):
            C_lambda_probs[i][j] = {}
            parse = C_lambda_dict[i][j].split(";")
            mu = float(parse[0])
            sig = float(parse[1])
            cdf_all = norm.cdf(arr, loc=mu, scale=sig)  # temp code, should be replaced with something more interesting
            for item1, item2 in zip(arr, cdf_all):
                C_lambda_probs[i][j][item1] = 1 - item2
    return C_lambda_probs


def create_C_lambda_probs_0_std(LHS, RHS, T, C, C_lambda_dict):
    C_lambda_probs = {}
    arr = np.asarray(range(T + C))
    for i in range(LHS):
        C_lambda_probs[i] = {}
        for j in range(RHS):
            C_lambda_probs[i][j] = {}
            parse = C_lambda_dict[i][j].split(";")
            mu = float(parse[0])
            if mu % 1 != 0:
                mu = int(mu) + 1
            for usage_time in arr:
                if usage_time < mu:
                    C_lambda_probs[i][j][usage_time] = 1.0
                else:
                    C_lambda_probs[i][j][usage_time] = 0.0

    return C_lambda_probs


if __name__ == '__main__':
    T = TICKS_IN_HOUR * 5
    C = 30
