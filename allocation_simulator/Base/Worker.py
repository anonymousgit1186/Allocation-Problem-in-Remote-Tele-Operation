import json


class Worker:

    ONLINE_STATUS_UPDATE_THRESHOLD = 35

    def __init__(self, lId, lName='', lSatisfaction='', lLocationSpecialty='', lTerrainSpecialty='',
                 lVehicleSpecialties='', lTimeOfTheDaySpecialties='', lWeatherSpecialties='',
                 lScenarioTypeSpecialities='', lTimeIdle='', lDurationTime='', lSeniority='', lAlertnessTest='',
                 lDemographicInformation=''):
        self.mId = lId
        self.mIndex = -1
        self.mName = lName

        self.mScenarioSatisfaction = lSatisfaction
        self.mLocationSpecialty = lLocationSpecialty
        self.mTerrainSpecialty = lTerrainSpecialty
        self.mVehicleSpecialties = lVehicleSpecialties
        self.mTimeOfTheDaySpecialties = lTimeOfTheDaySpecialties
        self.mWeatherSpecialties = lWeatherSpecialties
        self.mScenarioTypeSpecialities = lScenarioTypeSpecialities

        self.mTimeIdle = lTimeIdle
        self.mDurationTime = lDurationTime
        self.mSeniority = lSeniority
        self.mAlertnessTest = lAlertnessTest
        self.mDemographicInformation = lDemographicInformation

        self.mAssignedJobId = ''

        self.mBusyStartTime = -1
        self.mJobAssignedTime = -1

        self.lastOnlineStatusUpdateTick = 0
        self.lastOnlineStatusJobId = ''
        self.lastOnlineIndicatorsCount = 0

        self.mCompletedTime = -1

    def getId(self):
        return self.mId

    def getName(self):
        return self.mName

    def getIndex(self):
        return self.mIndex


    def getBusyStartTime(self):
        return self.mBusyStartTime

    def getJobAssignedTime(self):
        return self.mJobAssignedTime

    def setBusyStartTime(self, lBusyStartTime):

        self.mCompletedTime = -1

        self.mBusyStartTime = lBusyStartTime

    def setJobAssignedTime(self, lJobAssignedTime):
        self.mJobAssignedTime = lJobAssignedTime


    def getScenarioSatisfactionArr(self):
        return self.mScenarioSatisfaction

    def getScenarioSatisfactionRate(self, scenarioType=None):
        if (None == scenarioType):
            return 0

        return int(self.mScenarioSatisfaction[scenarioType])

    def getLocationSpecialty(self):
        return self.mLocationSpecialty

    def getTerrainSpecialty(self):
        return self.mTerrainSpecialty

    def getVehicleSpecialties(self):
        return self.mVehicleSpecialties

    def getTimeOfTheDaySpecialties(self):
        return self.mTimeOfTheDaySpecialties

    def getWeatherSpecialties(self):
        return self.mWeatherSpecialties

    def getScenarioTypeSpecialities(self):
        return self.mScenarioTypeSpecialities

    def getTimeIdle(self):
        return self.mTimeIdle

    def getDurationTime(self):
        return self.mDurationTime

    def getSeniority(self):
        return self.mSeniority

    def getAlertnessTest(self):
        return self.mAlertnessTest

    def getDemographicInformation(self):
        return self.mDemographicInformation

    def loadFromArr(self, workerDescriptionArr):
        self.mId = workerDescriptionArr['workerId']

        if 'index' in workerDescriptionArr:
            self.mIndex = workerDescriptionArr['index']

        if 'name' in workerDescriptionArr:
            self.mName = workerDescriptionArr['name']

        if 'scenarioSatisfaction' in workerDescriptionArr:
            self.mScenarioSatisfaction = workerDescriptionArr['scenarioSatisfaction']
            
        if 'locationSpecialty' in workerDescriptionArr:
            self.mLocationSpecialty = workerDescriptionArr['locationSpecialty']

        if 'terrainSpecialty' in workerDescriptionArr:
            self.mTerrainSpecialty = workerDescriptionArr['terrainSpecialty']

        if 'vehicleSpecialties' in workerDescriptionArr:
            self.mVehicleSpecialties = workerDescriptionArr['vehicleSpecialties']

        if 'timeOfTheDaySpecialties' in workerDescriptionArr:
            self.mTimeOfTheDaySpecialties = workerDescriptionArr['timeOfTheDaySpecialties']

        if 'weatherSpecialties' in workerDescriptionArr:
            self.mWeatherSpecialties = workerDescriptionArr['weatherSpecialties']

        if 'scenarioTypeSpecialities' in workerDescriptionArr:
            self.mScenarioTypeSpecialities = workerDescriptionArr['scenarioTypeSpecialities']

        if 'timeIdle' in workerDescriptionArr:
            self.mTimeIdle = workerDescriptionArr['timeIdle']

        if 'durationTime' in workerDescriptionArr:
            self.mDurationTime = workerDescriptionArr['durationTime']

        if 'seniority' in workerDescriptionArr:
            self.mSeniority = workerDescriptionArr['seniority']

        if 'alertnessTest' in workerDescriptionArr:
            self.mAlertnessTest = workerDescriptionArr['alertnessTest']

        if 'demographicInformation' in workerDescriptionArr:
            self.mDemographicInformation = workerDescriptionArr['demographicInformation']


    @staticmethod
    def toArray(worker):
        entityJson = {}

        entityJson['workerId'] = worker.getId()
        entityJson['name'] = worker.getName()
        entityJson['index'] = worker.getIndex()

        entityJson['scenarioSatisfaction'] = worker.getScenarioSatisfactionArr()
        entityJson['locationSpecialty'] = worker.getLocationSpecialty()
        entityJson['terrainSpecialty'] = worker.getTerrainSpecialty()
        entityJson['vehicleSpecialties'] = worker.getVehicleSpecialties()
        entityJson['timeOfTheDaySpecialties'] = worker.getTimeOfTheDaySpecialties()
        entityJson['weatherSpecialties'] = worker.getWeatherSpecialties()
        entityJson['scenarioTypeSpecialities'] = worker.getScenarioTypeSpecialities()
        entityJson['timeIdle'] = worker.getTimeIdle()
        entityJson['durationTime'] = worker.getDurationTime()
        entityJson['seniority'] = worker.getSeniority()
        entityJson['alertnessTest'] = worker.getAlertnessTest()
        entityJson['demographicInformation'] = worker.getDemographicInformation()

        return entityJson

    @staticmethod
    def toJson(worker):
        return json.dumps(Worker.toArray(worker))

    def onTick(self, currentTime=-1):
        pass

    def isBusy(self, currentTick):
        return self.mBusyStartTime > 0 and currentTick >= self.mBusyStartTime and self.mCompletedTime < 0

    def isAvailable(self, currentTick):
        return not self.isBusy(currentTick) and self.getJobAssignedTime() < 0

    def setAssignedJobId(self, lAssignedJobId):
        self.mAssignedJobId = lAssignedJobId

    def getAssignedJobId(self):
        return self.mAssignedJobId

    def getCompletedTime(self):
        return self.mCompletedTime

    def setNotBusy(self, currentTime):
        self.mCompletedTime = currentTime

        self.setAssignedJobId('')

        self.setJobAssignedTime(-1)

    def updateOnlineStatus(self, currentTime, currentReportedJobId, indicatorLedsCount = 0):
        self.lastOnlineStatusUpdateTick = currentTime
        self.lastOnlineStatusJobId = currentReportedJobId
        self.lastOnlineIndicatorsCount = indicatorLedsCount

    def getTimeOffline(self, currentTime):
        return (currentTime - self.lastOnlineStatusUpdateTick)

    def getLastOnlineStatusUpdateTick(self):
        return self.lastOnlineStatusUpdateTick

    def getLastOnlineStatusJobId(self):
        return self.lastOnlineStatusJobId

    def setLastOnlineStatusJobId(self, indicatorLedsCount = 0):
        self.lastOnlineIndicatorsCount = indicatorLedsCount

    def getLastOnlineIndicatorsCount(self):
        return self.lastOnlineIndicatorsCount

    def isOnline(self, currentTime):
        return self.getTimeOffline(currentTime) < self.ONLINE_STATUS_UPDATE_THRESHOLD

    def isOffline(self, currentTime):
        return not self.isOnline(currentTime)



    def dot(self, dot, currentTime):
        if not self.isAvailable(currentTime) != '':
            dot.node('W ' + str(self.mId), 'W ' + str(self.mId), color='red')
        else:
            dot.node('W ' + str(self.mId), 'W ' + str(self.mId), color='black')
