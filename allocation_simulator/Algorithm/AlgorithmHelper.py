import numpy as np

MAX_WAITING_TIME = 5
MIN_TIME = int(7 / 5)
MAX_TIME = int(288 / 5)
MEAN_TIME = 49.46666666666667
STD_TIME = 41.66206756227082
MIN_QUALITY = 0
MAX_QUALITY = 5
MIN_URGANCY = 1
MAX_URGANCY = 10
MAX_WT = 5


class AlgorithmHelper:
    
    # Used for real environment
    JOB_LOADING_TICKS = 0

    ALGORITHM_THRESHOLD = 0
    ALGORITHM_DELTA = 0.015

    def __init__(self):

        pass

    @staticmethod
    def calculateRealQuality(job, worker, scheduler):

        assignment = scheduler.getAssignmentByJobId(job.getId())

        if (None == assignment):
            assignment = scheduler.getCompletedAssignmentByJobId(job.getId())

        if (None == assignment):
            return 0

        if assignment.isJobCompleted():
            # return assignment.getQuality()
            return 5  # TODO:for now, until quality is updated in Assignment
        else:
            return None

    @staticmethod
    def predictQuality(job, worker, predicted_qualities, eps_percent):

        if not isinstance(job, str):
            job = job.getId()

        if not isinstance(worker, str):
            worker = worker.getId()

        if ((worker, job) not in predicted_qualities):
            print('Problem in predictQuality: ' + str(worker) + ',' + str(
                job) + ' not in predicted_qualities. Using 10 as real_q...', file=f)
            real_q = 10.0
        else:
            real_q = predicted_qualities[(worker, job)]
        # epsilon = random.uniform(-eps_percent * real_q, eps_percent * real_q)
        est_q = real_q  # + epsilon
        return est_q

    @staticmethod
    def calculateRealTime(job, worker, scheduler):

        assignment = scheduler.getCompletedAssignmentByJobId(job.getId())

        if None != assignment:
            return assignment.getActiveTime()
        else:

            print('Potential error in calculateRealTime. Assignment is null', file=f)

            return None

    @staticmethod
    def getFreeWorkers(workers, currentTime):

        free_workers = []

        for workerId, worker in workers.items():

            if worker.isAvailable(currentTime):
                free_workers.append(workerId)

        return free_workers

    def getJobsWorkersForThreshold(all_jobs, all_workers, currentTime):

        assign_now = {}
        available_now = {}

        for jobID, job in all_jobs.items():
            if job.getWaitingTime() >= MAX_WAITING_TIME:
                assign_now[jobID] = job

        if len(assign_now) > 0:

            for workerID, worker in all_workers.items():
                if worker.isAvailable(currentTime):
                    available_now[workerID] = worker

        return assign_now, available_now

    @staticmethod
    def predictTime(job, worker, predicted_times, getSimulatedTime=False):

        if not isinstance(job, str):
            job = job.getId()

        if not isinstance(worker, str):
            worker = worker.getId()

        if ((worker, job) not in predicted_times):
            print('Problem in predictTime: ' + str(worker) + ',' + str(
                job) + ' not in predicted_times. Using 10 as est_time...', file=f)
            est_time = 10.0
        else:
            est_time = predicted_times[(worker, job)][0]
            simulated_time = predicted_times[(worker, job)][1]

        # epsilon = random.uniform(-eps_percent * real_time, eps_percent * real_time)
        # est_time = real_time + epsilon

        if (getSimulatedTime):
            if simulated_time == -1:
                # print('Problem in simulated_time: ' + str(worker) + ',' + str(
                #     job) + ' not in simulated_times. Using predicted_times...')
                simulated_time = est_time
            return simulated_time
        else:
            return est_time

    @staticmethod
    def normalizeTime(x, mean=None, std=None):

        if x == 0:
            return 0

        x = np.log(x)

        return (x - np.log(MIN_TIME)) / (np.log(MAX_TIME) - np.log(MIN_TIME))

    @staticmethod
    def normalizeQuality(x):
        return (x - MIN_QUALITY) / (MAX_QUALITY - MIN_QUALITY)

    @staticmethod
    def normalizeUrgency(x):
        return (x - MIN_URGANCY) / (MAX_URGANCY - MIN_URGANCY)

    @staticmethod
    def normalizeWaitingTime(x, maxWT):
        if x ==0:
            return 0
        return (np.log(x) - 0) / (np.log(maxWT - 1) - 0)

    @staticmethod
    def getAlgorithmValue(C,job, worker, scheduler, alpha, eps, type, isWT, currentTime, predicted_times,
                          predicted_qualities, waiting_match):

        remained_time = 0

        if not worker.isAvailable(currentTime):
            assignments = scheduler.getAssignments()
            for ass in assignments.values():
                if worker.getId() == ass.getWorkerId():
                    # remained_time = ass.getRemaindTime()
                    remained_time = int(AlgorithmHelper.predictTime(ass.getJobId(), worker, predicted_times,
                                                                    getSimulatedTime=False)) - int(ass.getActiveTime())

                    # if the prediction was lower than reality, remained time is 1 tick
                    if remained_time <= 0:
                        remained_time = 1
                    break

        if waiting_match:
            if worker.getId() in waiting_match.keys():
                for jobsID in waiting_match[worker.getId()]:
                    remained_time += AlgorithmHelper.predictTime(jobsID, worker, predicted_times, getSimulatedTime=False)

        norm_t = AlgorithmHelper.normalizeTime(
            AlgorithmHelper.predictTime(job, worker, predicted_times, getSimulatedTime=False)) + AlgorithmHelper.normalizeTime(remained_time)
        norm_u = AlgorithmHelper.normalizeUrgency(job.getUrgency())
        norm_q = 0

        if 'constrains' not in type:
            norm_q = AlgorithmHelper.normalizeQuality(
                AlgorithmHelper.predictQuality(job, worker, predicted_qualities, eps))
            print('WRONG behaviour', file=f)

        if 'TT' in type:
            norm_u = 0
            norm_q = 0

        if isWT:
            norm_wt = AlgorithmHelper.normalizeWaitingTime(job.getWaitingTime(), C)

            value = ((-alpha[0]) * norm_t) + \
                    (alpha[1] * norm_q) + \
                    (alpha[2] * norm_u) + \
                    (alpha[5] * norm_wt)
        else:
            value = ((-alpha[0]) * (norm_t)) + \
                    (alpha[1] * norm_q) + \
                    (alpha[2] * norm_u)

        return value

    @staticmethod
    def getAlgorithmEvaluation(job, worker, alpha, type_val_func, scheduler, C):

        norm_t = AlgorithmHelper.normalizeTime(AlgorithmHelper.calculateRealTime(job, worker, scheduler))
        norm_u = AlgorithmHelper.normalizeUrgency(job.getUrgency())
        norm_wt = AlgorithmHelper.normalizeWaitingTime(job.getWaitingTime(), C)
        norm_q = 0

        if 'constrains' not in type_val_func:
            norm_q = AlgorithmHelper.normalizeQuality(AlgorithmHelper.calculateRealQuality(job, worker, scheduler))
            print('WRONG behaviour', file=f)

        if 'TT' in type_val_func:
            norm_u = 0
            norm_q = 0

        value = ((-alpha[0]) * norm_t) + \
                (alpha[1] * norm_q) + \
                (alpha[2] * norm_u) - \
                (alpha[5] * norm_wt)

        return value
