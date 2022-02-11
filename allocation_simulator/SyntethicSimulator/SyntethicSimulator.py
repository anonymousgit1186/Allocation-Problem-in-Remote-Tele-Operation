from time import sleep
import pandas as pd
from graphviz import Digraph
import os
import csv
import json
import sys
from datetime import datetime
from Base.Statistics import Statistics
from Base.Scheduler import Scheduler
from Base.JobsManager import JobsManager
from online_matching import print_allocation_value, run_algo, OUTPUT_DIR_NAME

sys.path.insert(1, os.path.join(os.path.dirname(__file__), '../LP_matching/'))

from LP_matching import LP_matching


class SyntethicSimulator:

    def __init__(self, lMissionsWebServer, lJobsManager, lWorkersManager, scheduler, isSimulationMode,
                 singleOperatorDebugMode=False):

        self.mSystemTick = 0

        self.isSimulationMode = isSimulationMode

        self.mMissionsWebServer = lMissionsWebServer

        self.mJobsManager = lJobsManager
        self.mWorkersManager = lWorkersManager
        self.waiting_match = {}
        self.mScheduler = scheduler

        # true for debug only
        self.singleOperatorDebugMode = singleOperatorDebugMode

        self.workersOfflineStatuses = {}
        self.pauseEventsRecords = []

    def tick(self):

        self.mSystemTick += 1

    def getCurrentSystemTick(self):

        return self.mSystemTick

    def run(self, alphas, epsilon, type_val_func, algo_type, isWT, sampled_times, predicted_times, predicted_qualities,\
            num_workers,\
            scenarioId, num_jobs,\
            statistics,\
            competitiveRatio,\
            C,\
            T,\
            solver_path,\
            ff,\
            secondsToSleepAfterEachTick=0.00001, verboseMode=False):

        global OUTPUT_DIR_NAME
        if OUTPUT_DIR_NAME == 0:
            OUTPUT_DIR_NAME = "{}_workers/scenario_{}".format(num_workers, scenarioId)

        df_ass = pd.DataFrame()
        self.waiting_match = {}
        hungarian_times = []
        WT = "" if isWT else "_noWT"
        dir_path = './debug/edge_weights/' + OUTPUT_DIR_NAME + '/'

        isPause = False

        eval_list_every_10_ass = {}
        tt_every_10_ass = {}
        last_completed = 0
        LP_AssignmentsList = None
        if ('LP' == algo_type):
            
            LHS, RHS, pjt, C_lambda_probs = LP_matching.init_data(ff,T, C, self.mWorkersManager.getWorkersIdsList())

            LP_AssignmentsList = [[[] for i in range(LHS)] for t in range(T)]

            alreadyAssignedJobsIndices = {}

            Xopt = LP_matching.solveLP(solver_path)

        arrivedJobsCount = 0
        completedJobsCount = 0

        while self.mSystemTick < T or \
                (len(self.mScheduler.getAssignments()) != 0 or len(
                    JobsManager.futureJobs(self.mJobsManager.getJobsList(),
                                           self.mSystemTick)) != 0 or self.mJobsManager.getWaitingJobsCount() != 0):
            # DEBUG PRINT
            print('Current system tick: ' + str(self.mSystemTick), file=ff)

            # Filter jobs by `start time >= current time` and `unassigned only`
            filteredJobsList = JobsManager.filterJobs(self.mJobsManager.getJobsList(), self.mSystemTick, True)

            all_jobs = filteredJobsList

            all_workers = self.mWorkersManager.getWorkersList()

            # pause when a worker is offline to long (used in real world scenario)
            for worker in all_workers.values():

                if (not self.isSimulationMode):
                    if (worker.isOffline(self.mSystemTick)):
                        print('worker is offline: ' + worker.getId(), file=ff)
                    else:
                        print('worker is online: ' + worker.getId(),file=ff)

                    if (not worker.isAvailable(self.mSystemTick)):
                        print('worker is busy: ' + worker.getId(),file=ff)
                    else:
                        print('worker is free: ' + worker.getId(),file=ff)

                workerIsOfflineNow = worker.isOffline(self.mSystemTick)

                if (workerIsOfflineNow):

                    if (worker.getId() not in self.workersOfflineStatuses):
                        self.workersOfflineStatuses[worker.getId()] = []

                    self.workersOfflineStatuses[worker.getId()].append(self.mSystemTick)

                    statistics.writeWorkerOnlineStatus(worker.getId(), True, (not worker.isAvailable(self.mSystemTick)),
                                                       worker.getAssignedJobId(),
                                                       worker.getTimeOffline(self.mSystemTick),
                                                       worker.getLastOnlineStatusUpdateTick(),
                                                       worker.getLastOnlineIndicatorsCount(),
                                                       worker.getLastOnlineStatusJobId(), self.mSystemTick)

                else:

                    statistics.writeWorkerOnlineStatus(worker.getId(), False,
                                                       (not worker.isAvailable(self.mSystemTick)),
                                                       worker.getAssignedJobId(),
                                                       worker.getTimeOffline(self.mSystemTick),
                                                       worker.getLastOnlineStatusUpdateTick(),
                                                       worker.getLastOnlineIndicatorsCount(),
                                                       worker.getLastOnlineStatusJobId(), self.mSystemTick)

                if not self.isSimulationMode and \
                        not self.singleOperatorDebugMode and \
                        not worker.isAvailable(self.mSystemTick) and \
                        self.mSystemTick > 60 and \
                        workerIsOfflineNow:

                    isPause = True

                    self.pauseEventsRecords.append(self.mSystemTick)

                    statistics.writeSystemPauseStatus(True, worker.getId(), self.mSystemTick)

                    for job in JobsManager.futureJobs(self.mJobsManager.getJobsList(), self.mSystemTick).values():
                        job.postponeArrival()

            # free pause when worker is back online
            isJustBackToOnline = False

            if isPause:

                print('SYSTEM IN PAUSE!', file=ff)

                if all([worker.isOnline(self.mSystemTick) for worker in all_workers.values()]):
                    isPause = False

                    isJustBackToOnline = True

                    print('pause finished', file=ff)

            if not isPause:

                if (not self.isSimulationMode):
                    print('system is online', file=ff)

                if (isJustBackToOnline):
                    print('isJustBackToOnline is true', file=ff)

                if ('LP' == algo_type):

                    if self.mSystemTick < T:
                        filteredJobsList = JobsManager.filterJobs(self.mJobsManager.getJobsList(), self.mSystemTick,
                                                                  True)

                        print(len(list(filteredJobsList.values())), file=ff)

                        LP_AssignmentsList, alreadyAssignedJobsIndices = LP_matching.runAlgorithm(ff,predicted_times,
                                                                                                       predicted_qualities,
                                                                                                       LP_AssignmentsList,
                                                                                                       alreadyAssignedJobsIndices,
                                                                                                       self.mSystemTick,
                                                                                                       filteredJobsList,
                                                                                                       LHS, RHS, pjt, T,
                                                                                                       C_lambda_probs,
                                                                                                       competitiveRatio,
                                                                                                       C,
                                                                                                       Xopt,
                                                                                                       self.mJobsManager,
                                                                                                       self.mWorkersManager,
                                                                                                       isWT,
                                                                                                       type_val_func,
                                                                                                       self.mScheduler,
                                                                                                       statistics)

                else:

                    self.waiting_match, df_ass, h_time = run_algo(algo_type, all_jobs, all_workers, self.waiting_match,
                                                                  self.mScheduler,
                                                                  self.mSystemTick, self.mJobsManager,
                                                                  self.mWorkersManager,
                                                                  alphas,
                                                                  epsilon, type_val_func, None, df_ass, isWT,
                                                                  sampled_times,
                                                                  predicted_times, predicted_qualities, C,
                                                                  self.mJobsManager, num_workers,
                                                                  scenarioId, num_jobs, statistics,
                                                                  self.isSimulationMode, isJustBackToOnline, ff)
                    hungarian_times.extend(h_time)

                statistics.writeSystemPauseStatus(False, '-', self.mSystemTick)


            filteredJobsListAfterAssignment = JobsManager.filterJobs(self.mJobsManager.getJobsList(), self.mSystemTick,
                                                                     True)

            statistics.writeWaitingJobsIds(filteredJobsListAfterAssignment.keys(), self.mSystemTick)


            print(self.mScheduler, file=ff)

            # Tick the simulator
            self.tick()

            self.mWorkersManager.onTick(self.mSystemTick)

            # Tick all instances
            self.mScheduler.onTick(self.mSystemTick, self.mJobsManager, self.mWorkersManager)

            self.mJobsManager.onTick(statistics, C, self.mSystemTick)

            filteredJobsListAfterAssignment = JobsManager.filterJobs(self.mJobsManager.getJobsList(), self.mSystemTick,
                                                                     True)

            endExperimentSignalReceived = False

            if (not self.isSimulationMode and None != self.mMissionsWebServer):

                if (verboseMode):
                    print('Updating subscribed clients', file=ff)

                self.mMissionsWebServer.updateCurrentSystemTick(self.getCurrentSystemTick())

                # Update subscribed clients
                endExperimentSignalReceived = self.mMissionsWebServer.spinOnce()
            else:

                # While mMissionsWebServer.spinOnce() is running - 
                # it cares abount the sleep (instead of sleep it listens for subscribed clients' requests), so
                # you do not need sleep(secondsToSleepAfterEachTick)

                # Controls speed of the simulation
                sleep(int(secondsToSleepAfterEachTick))

            if (endExperimentSignalReceived):
                print('Finishing the simulation on end signal', file=ff)
                break

        statistics.writePauseEventsRecords(self.pauseEventsRecords, False, 'a+')
        statistics.writeWorkersOfflineStatuses(self.workersOfflineStatuses, False, 'a+')

        if LP_AssignmentsList:
            statistics.writeAssignmentsListAti(LP_AssignmentsList, self.mWorkersManager)


        alloc_val, mean_tt, max_tt = print_allocation_value(statistics, self.mScheduler, self.mJobsManager,
                                                            self.mWorkersManager,
                                                            alphas, epsilon,
                                                            type_val_func, isWT, algo_type, self.mSystemTick, C, ff)


        return alloc_val, mean_tt, max_tt, None, self.mSystemTick - 1, hungarian_times
