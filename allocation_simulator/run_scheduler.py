#!/usr/bin/env python
# coding: utf-8

import sys
import csv
import random
from datetime import datetime
import numpy as np
import pandas as pd
import scipy.stats as stats
import time
import os
import signal

sys.dont_write_bytecode = True
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'Base'))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'SyntethicSimulator'))
sys.path.insert(1, os.path.join(os.path.dirname(__file__), 'LP_matching'))

from Base.Statistics import Statistics
from BaseManager import BaseManager

from WorkersManager import WorkersManager
from JobsManager import JobsManager
from Scheduler import Scheduler
from online_matching import scenarios_dict, COL_PARTICIPANT_ID, COL_SUFFIX_TIME

from SyntethicSimulator import SyntethicSimulator


from LP_matching import LP_matching


def run_scheduler(scenarioId, algorightmToUse, isTrainingMode, isWT, jobsDescriptionTableFilePath,
                  workersDescriptionTableFilePath, timeQualityPath, statistics, competitiveRatio, C, T,
                  isSimulationMode, secondsToSleepAfterEachTick, solver_path,f, verboseMode=False):

    print("PARAMETERS: " "scid {} algo {} is_train {} isWt {} JDESCR PATH {} wdescpath {} timequal path {} stats {} compet {} C {} T {} is sim {} sleep after tick {} spath {}, verbose {}".format(scenarioId, algorightmToUse, isTrainingMode, isWT, jobsDescriptionTableFilePath, workersDescriptionTableFilePath, timeQualityPath, statistics, competitiveRatio, C, T,isSimulationMode, secondsToSleepAfterEachTick, solver_path, verboseMode),file=f)
    sampled_times = None
    predicted_times = None
    epsilon = 0.15
    alpha = [0.7, 0.4, 0.4, 0, 0.2, 0.6]
    results, run_times, mean_tt_dict, max_tt_dict, num_hungarian_dict, simul_length_dict, h_avg_time_dict, h_tt_dict = {}, {}, {}, {}, {}, {}, {}, {}

    if (verboseMode):
        print('Algorithm: ' + algorightmToUse,file=f)

    jobsManager = JobsManager(f)
    workersManager = WorkersManager()

    jobsManager.addJobsFromCSV(jobsDescriptionTableFilePath)
    workersManager.addWorkersFromCSV(workersDescriptionTableFilePath)

    #random.seed(5 * workersManager.getWorkersListLength())

    if (verboseMode):
        print("workers: ", workersManager.getWorkersList().keys(),file=f)

    if not sampled_times:
        predicted_qualities = {}
        predicted_times = {}  # from training

        if (not isTrainingMode):

            exp_data = pd.read_csv(timeQualityPath)
            for index, row in exp_data.iterrows():
                job_id = row['job_id']
                worker_id = row['worker_id']

                simulatedTime = -1

                if ('simulated_time' in row):
                    simulatedTime = int(row['simulated_time'])

                predicted_times[(worker_id, job_id)] = (int(row['expected_time']), simulatedTime)
                predicted_qualities[(worker_id, job_id)] = int(row['expected_quality'])

        else:
            for workerId, worker in workersManager.getWorkersList().items():
                for jobId, job in jobsManager.getJobsList().items():
                    predicted_times[(workerId, jobId)] = 0
                    predicted_qualities[(workerId, jobId)] = 5

    scheduler = Scheduler(isSimulationMode, statistics, f)

    if (not isSimulationMode):
        missionsWebServer = MissionsWebServer(secondsToSleepAfterEachTick, workersManager, jobsManager, scheduler,
                                              statistics)
    else:
        missionsWebServer = None

    singleOperatorDebugMode = isTrainingMode

    syntethicSimulator = SyntethicSimulator(missionsWebServer, jobsManager, workersManager, scheduler, isSimulationMode,
                                            singleOperatorDebugMode)

    start_time = time.time()
    alloc_val, mean_tt, max_tt, num_of_hungarian, simul_length, h_times = syntethicSimulator.run(alpha, epsilon,
                                                                                                 'TT_constrains',
                                                                                                 algorightmToUse,
                                                                                                 isWT,
                                                                                                 sampled_times,
                                                                                                 predicted_times,
                                                                                                 predicted_qualities,
                                                                                                 workersManager.getWorkersListLength(),
                                                                                                 '',
                                                                                                 jobsManager.getJobsListLength(),
                                                                                                 statistics,
                                                                                                 competitiveRatio,
                                                                                                 C, T,
                                                                                                 solver_path, 
                                                                                                 f,
                                                                                                 secondsToSleepAfterEachTick,
                                                                                                                verboseMode)
    
    print('alloc_val: ' + str(alloc_val),file=f)
    statsArr = {}
    statsArr['end_time'] = time.time()
    statsArr['h_sum_times'] = sum(h_times)
    statsArr['h_avg_time'] = statsArr['h_sum_times'] / len(h_times) if len(h_times) != 0 else 0
    statsArr['algo_name'] = algorightmToUse
    statsArr['algo_name'] += '' if isWT else '_noWT'
    statsArr['results.keys'] = str(list(results.keys())).replace(',', ';')
    statsArr['results'] = alloc_val
    statsArr['mean_tt_dict'] = mean_tt
    statsArr['max_tt_dict'] = max_tt
    statsArr['num_hungarian_dict'] = num_of_hungarian
    statsArr['simul_length_dict'] = simul_length
    statsArr['run_times'] = statsArr['end_time'] - start_time
    statsArr['h_avg_time_dict'] = statsArr['h_avg_time']
    statsArr['h_tt_dict'] = statsArr['h_sum_times']

    # cumulativeMeanRowStr = '\n'
    # cumulativeMeanRowStr += 'Cumulative waiting times mean,'
    # cumulativeMeanRowStr += str(np.amin(questionWaitingTimesCumulativeMean)) + ','
    # cumulativeMeanRowStr += str(np.amax(questionWaitingTimesCumulativeMean)) + ','
    # cumulativeMeanRowStr += str(np.mean(questionWaitingTimesCumulativeMean)) + ','
    # cumulativeMeanRowStr += str(np.std(questionWaitingTimesCumulativeMean)) + ','
    # cumulativeMeanRowStr += str(np.var(questionWaitingTimesCumulativeMean)) + ','
    # cumulativeMeanRowStr += str(len(questionWaitingTimesCumulativeMean)) + ','

    statistics.writeExperimentSummaryStats(statsArr, 'w+')

    return statsArr

def run(args):

        argv = args

        print(argv)

        scenarioId = argv[0]
        algorightmToUse = argv[1]
        isTrainingMode = 'true' == argv[2]
        jobsDescriptionTableFilePath = argv[3]
        workersDescriptionTableFilePath = argv[4]
        timeQualityPath = argv[5]
        run_index = argv[8]
        solver_path = argv[10]
       
        isWT = 'WT' == argv[9]
        
        with open(run_index + ".output", "w") as f:

            tripDataDirectoryIndex = '1'

            if (len(argv) > 6):
                C = int(argv[6])
            else:
                C = 6

            if (len(argv) > 7):
                T = int(argv[7])
            else:
                T = 720 * 24 + 5

            competitiveRatio = 0.5

            # For real time: 1 tick <- 1 second
            secondsToSleepAfterEachTick = 0.00001

            # Whether the jobs completed by the simulator (true) or by MissionsWebServer (false)
            isSimulationMode = True

            verboseMode = True

            trainingModeDirNameStr = 'real_mode'

            if (isTrainingMode):
                trainingModeDirNameStr = 'training_mode'

            WT = "WT" if isWT else "_noWT"
            TQ ='sameMuExp' if 'sameMuExp' in timeQualityPath else 'diffMuExp'
            scenario_name = jobsDescriptionTableFilePath.split('/')[-1].split('.')[0]
            num_workers = workersDescriptionTableFilePath.split('/')[-1].split('.')[0].split('_')[-2]
            executionUniqueIndex = scenario_name+'_'+num_workers + 'w'+str(C - 1) + 'q5H/'+algorightmToUse + '_'+scenario_name+'_' + num_workers + 'w_' + str(C - 1) + 'queue_run_' + str(run_index) + '_' + WT + '_'+TQ +'_'+ datetime.utcnow().strftime('%d-%m-%y.%H-%M-%S.%f')[:-3]

            statistics = Statistics(executionUniqueIndex)

            statistics.writeWorkersOfflineStatuses({}, True, 'w+')
            statistics.writePauseEventsRecords({}, True, 'w+')
            statistics.writeAlgorithmDebugRow({}, True, 'w+')
            statistics.writeAssignmentLog({}, True, 'w+')
            statistics.writeWorkerOnlineStatusTitle('w+')
            statistics.writeWaitingJobsIdsTitle('w+')
            statistics.writeRejectedJobsTitle('w+')
            statistics.writeSystemPauseStatusTitle('w+')
            statistics.writeJobCompletionByOperatorFileTitle('w+')
           
            statsArr = run_scheduler(scenarioId, algorightmToUse, isTrainingMode, isWT, jobsDescriptionTableFilePath,
                                     workersDescriptionTableFilePath, timeQualityPath, statistics, competitiveRatio, C, T,
                                     isSimulationMode, secondsToSleepAfterEachTick, solver_path, f, verboseMode)

            print(statsArr, file=f)

            statistics.writeTimeQualityStats(statsArr, 'w+')


        return 0
