#!/usr/bin/env python
import sys
import random
import time
import os
import signal

sys.path.insert(1, './Base')

from WorkersManager import WorkersManager
from Job import Job
from JobsManager import JobsManager

from functools import partial
from json import dumps
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer

# import subprocess

class VarHolder():
    
    def __init__(self, var):
        self.var = var

    def get(self):
        return self.var

    def set(self, newVal):
        self.var = newVal
    
class MissionsWebServer():

    def __init__(self, timeout, workersManager, jobsManager, scheduler, statistics, file=f,port = 15077):
        self.f = f
        self.port = port

        self.statistics = statistics

        print('MissionsWebServer is listening on localhost:%s' % port, file=f)

        self.currentTickObj = VarHolder(0)

        handler = partial(RequestHandler, workersManager, jobsManager, scheduler, self.currentTickObj, self.statistics)

        self.server = ThreadingHTTPServer(('', port), handler)

        if (timeout <= 0.1):
            timeout = 0.1

        self.requestedTimeout = timeout - 0.003

        self.server.timeout = 0.24
        # server.serve_forever()

        self.endExperimentSignalReceived = False
        self.setupSignals()

    def setupSignals(self):
        signal.signal(signal.SIGINT, self.signalsHandler)
        signal.siginterrupt(signal.SIGINT, False)
        signal.signal(signal.SIGTERM, self.signalsHandler)
        signal.siginterrupt(signal.SIGTERM, False)

    def signalsHandler(self, signum, frame):
        self.endExperimentSignalReceived = True

    def updateCurrentSystemTick(self, currentSystemTick):

        self.currentTickObj.set(currentSystemTick)

    def spinOnce(self):
        
        elapsedTime = 0

        iterations = 1
        averageHandlingTime = 0

        while (elapsedTime + averageHandlingTime) < self.requestedTimeout:

            startTime = time.time()

            self.server.handle_request()

            singleHandlingTime = time.time() - startTime

            averageHandlingTime = (averageHandlingTime * (iterations - 1) / iterations) + singleHandlingTime / iterations

            elapsedTime += singleHandlingTime

        print('elapsedTime: ' + str(elapsedTime), file=self.f)

        return self.endExperimentSignalReceived

        

class RequestHandler(BaseHTTPRequestHandler):

    def __init__(self, workersManager, jobsManager, scheduler, currentTickVar, statistics, *args, **kwargs):
        self.currentSystemTick = currentTickVar
        self.workersManager = workersManager
        self.jobsManager = jobsManager
        self.scheduler = scheduler
        self.statistics = statistics
        # BaseHTTPRequestHandler calls do_GET **inside** __init__
        super().__init__(*args, **kwargs)

    def _send_cors_headers(self):
        """ Sets headers required for CORS """
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "x-api-key,Content-Type")

    def send_dict_response(self, d):
        """ Sends a dictionary (JSON) back to the client """
        self.wfile.write(bytes(dumps(d), "utf8"))

    def log_request(self, format, *args):
        return
        
    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()


    def do_GET(self):
        
        request_path = self.path

        if (request_path.startswith('/set_job_completed?jobId=')):

            self.send_response(200)
            self._send_cors_headers()
            self.end_headers()

            jobId = request_path.split('/set_job_completed?jobId=', 1)[1]
            
            self.scheduler.updateJobCompletionStatus(jobId, True, self.currentSystemTick.get())
            
            return
            
        
        if (request_path.startswith('/set_job_fetched?jobId=')):

            self.send_response(200)
            self._send_cors_headers()
            self.end_headers()

            jobId = request_path.split('/set_job_fetched?jobId=', 1)[1]

            # +1 because it will be completed in the next tick
            job = self.jobsManager.getJobById(jobId)
            
            self.scheduler.updateJobFetchTime(jobId, job, self.currentSystemTick.get() + 1)
            
            return
            
        if (request_path.startswith('/set_job_started?jobId=')):

            self.send_response(200)
            self._send_cors_headers()
            self.end_headers()

            jobId = request_path.split('/set_job_started?jobId=', 1)[1]

            job = self.jobsManager.getJobById(jobId)

            # +1 because it will be completed in the next tick
            self.scheduler.updateJobStartTime(jobId, job, self.currentSystemTick.get() + 1)
            
            return
            
        if (request_path.startswith('/online_status_update?operatorId=')):

            self.send_response(200)
            self._send_cors_headers()
            self.end_headers()

            operatorIdAndJobId = request_path.split('/online_status_update?operatorId=', 1)[1]


            if ('*' == operatorIdAndJobId):
                
                self.workersManager.updateAllWorkersOnlineStatus(self.currentSystemTick.get())
                return


            currentOperatorJobId = ''

            operatorId = operatorIdAndJobId.split('&jobId=')[0]
            
            currentOperatorJobId = '-'
            indicatorLedsCount = 0

            if (len(operatorIdAndJobId.split('&')) > 1):

                if (len(operatorIdAndJobId.split('&jobId=')[1].split('&')) > 0):
                    currentOperatorJobId = operatorIdAndJobId.split('&jobId=')[1].split('&')[0]

                if (len(operatorIdAndJobId.split('&indicatorLedsCount=')) > 1):
                    indicatorLedsCount = int(operatorIdAndJobId.split('&indicatorLedsCount=')[1])

            operator = self.workersManager.getWorkerById(operatorId)

            if (None != operator and indicatorLedsCount > 4):
                operator.updateOnlineStatus(self.currentSystemTick.get(), currentOperatorJobId, indicatorLedsCount)
            else:
                if (None != operator):
                    operator.setLastOnlineStatusJobId(indicatorLedsCount)
            
            return

        if (request_path.startswith('/is_operator_online?operatorId=')):

            self.send_response(200)
            self._send_cors_headers()
            self.end_headers()

            operatorId = request_path.split('/is_operator_online?operatorId=', 1)[1]

            operator = self.workersManager.getWorkerById(operatorId)

            if (None != operator):
                if (operator.isOnline(self.currentSystemTick.get())):
                    self.wfile.write(bytes(str(operator.getLastOnlineIndicatorsCount()), 'utf-8'))
                else:
                    self.wfile.write(bytes('false,'+str(operator.getLastOnlineIndicatorsCount()), 'utf-8'))
            
            return

        if ('/get_waiting_jobs_list' == request_path):

            self.send_response(200)
            self._send_cors_headers()
            self.end_headers()

            response = {}
            response['jobs'] = {}

            for jobId, job in self.jobsManager.getJobsList().items():

                if (job.isAssigned()):
                    continue

                if (int(job.getArrivalTime()) > int(self.currentSystemTick.get())):
                    continue

                response['jobs'][jobId] = Job.toArray(job)
                
                response['jobs'][jobId]['jobArrivalTime'] = job.getArrivalTime()
                response['jobs'][jobId]['jobWaitingTime'] = job.getWaitingTime()
                response['jobs'][jobId]['jobUrgency'] = job.getUrgency()

            response['currentTick'] = self.currentSystemTick.get()

            self.send_dict_response(response)

            return

        if (request_path.startswith('/get_operator_jobs_list?operatorId=')):

            self.send_response(200)
            self._send_cors_headers()
            self.end_headers()

            operatorId = request_path.split('/get_operator_jobs_list?operatorId=', 1)[1]

            response = {}

            for jobId, job in self.jobsManager.getJobsList().items():

                if (operatorId != job.getAssignedWorkerId()):
                    continue

                response[jobId] = Job.toArray(job)
                
                response[jobId]['jobArrivalTime'] = job.getArrivalTime()
                response[jobId]['jobWaitingTime'] = job.getWaitingTime()
                response[jobId]['jobUrgency'] = job.getUrgency()

                jobScenarioParams = job.getScenarioParams()

                for scenarioParamName in jobScenarioParams:
                    response[jobId][scenarioParamName] = jobScenarioParams[scenarioParamName]

            self.send_dict_response(response)

            return


        self.send_response(404)
