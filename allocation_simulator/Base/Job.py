import json


class Job:

    def __init__(self, lId, lIndex, f, lName='', lRepetative='', lMultiplier='', lArrivalTime=0, lMaxStartDelay=0, lUrgency=0,
                 lMinTimeForHandling=0, lMaxTimeForHandling=0, lLocation='', lTerrain='', lVehicle='', lTimeOfTheDay='',
                 lWeather='', lScenarioType='', lScenarioParams = {}):
        self.mId = lId
        self.mIndex = lIndex

        self.mName = lName
        self.mRepetative = lRepetative
        self.mMultiplier = lMultiplier
        self.mArrivalTime = int(float(lArrivalTime))
        self.mMaxStartDelay = int(float(lMaxStartDelay))
        self.mUrgency = int(float(lUrgency))
        self.mMinTimeForHandling = int(float(lMinTimeForHandling))
        self.mMaxTimeForHandling = int(float(lMaxTimeForHandling))
        self.mLocation = lLocation
        self.mTerrain = lTerrain
        self.mVehicle = lVehicle
        self.mTimeOfTheDay = lTimeOfTheDay
        self.mWeather = lWeather
        self.mScenarioType = lScenarioType

        self.mDedicatedToOperatorIndex = -1

        self.mWaitingTime = 0
        self.mActiveTime = 0
        self.mCompletedTime = 0
        self.mIsAssigned = ''
        self.mRandomRealTime = None

        self.mAssignedTime = -1
        self.mFetchedTime = -1
        self.mWorkStartedTime = -1
        self.f = f
        self.mScenarioParams = lScenarioParams

    def getId(self):
        return self.mId

    def getIndex(self):
        return self.mIndex

    def getName(self):
        return self.mName

    def getRepetative(self):
        return self.mRepetative

    def getScenarioParams(self):
        return self.mScenarioParams

    def getMultiplier(self):
        return self.mMultiplier

    def getArrivalTime(self):
        return self.mArrivalTime

    def getMaxStartDelay(self):
        return self.mMaxStartDelay

    def getWaitingTime(self):
        return self.mWaitingTime

    def getUrgency(self):
        return self.mUrgency

    def getMinTimeForHandling(self):
        return self.mMinTimeForHandling

    def getMaxTimeForHandling(self):
        return self.mMaxTimeForHandling

    def getLocation(self):
        return self.mLocation

    def getTerrain(self):
        return self.mTerrain

    def getVehicle(self):
        return self.mVehicle

    def getTimeOfTheDay(self):
        return self.mTimeOfTheDay

    def getWeather(self):
        return self.mWeather

    def getScenarioType(self):
        return self.mScenarioType

    def getDedicatedOperatorIndex(self):
        return self.mDedicatedToOperatorIndex

    def getCompletedTime(self):
        return self.mCompletedTime

    def loadFromArr(self, jobDescriptionArr):

        if ('jobId' in jobDescriptionArr and None != jobDescriptionArr['jobId']):
            self.mId = jobDescriptionArr['jobId']
        
        if ('job_id' in jobDescriptionArr and None != jobDescriptionArr['job_id']):
            self.mId = jobDescriptionArr['job_id']
        
        if ('name' in jobDescriptionArr and None != jobDescriptionArr['name']):
            self.mName = jobDescriptionArr['name']
        
        if ('repetative' in jobDescriptionArr and None != jobDescriptionArr['repetative']):
            self.mRepetative = "true" == jobDescriptionArr['repetative']
        
        if ('multiplier' in jobDescriptionArr and None != jobDescriptionArr['multiplier']):
            self.mMultiplier = jobDescriptionArr['multiplier']
        
        if ('arrivalTime' in jobDescriptionArr and None != jobDescriptionArr['arrivalTime']):
            self.mArrivalTime = int(float(jobDescriptionArr['arrivalTime']))
        
        if ('arrival_time' in jobDescriptionArr and None != jobDescriptionArr['arrival_time']):
            self.mArrivalTime = int(float(jobDescriptionArr['arrival_time']))
        
        if ('maxStartDelay' in jobDescriptionArr and None != jobDescriptionArr['maxStartDelay']):
            self.mMaxStartDelay = int(float(jobDescriptionArr['maxStartDelay']))
        
        if ('waitingTime' in jobDescriptionArr and None != jobDescriptionArr['waitingTime']):
            self.mWaitingTime = int(float(jobDescriptionArr['waitingTime']))
        
        if ('activeTime' in jobDescriptionArr and None != jobDescriptionArr['activeTime']):
            self.mActiveTime = int(float(jobDescriptionArr['activeTime']))
        
        if ('completedTime' in jobDescriptionArr and None != jobDescriptionArr['completedTime']):
            self.mCompletedTime = int(float(jobDescriptionArr['completedTime']))
        
        if ('urgency' in jobDescriptionArr and None != jobDescriptionArr['urgency']):
            self.mUrgency = int(float(jobDescriptionArr['urgency']))
        
        if ('minTimeForHandling' in jobDescriptionArr and None != jobDescriptionArr['minTimeForHandling']):
            self.mMinTimeForHandling = int(float(jobDescriptionArr['minTimeForHandling']))
        
        if ('maxTimeForHandling' in jobDescriptionArr and None != jobDescriptionArr['maxTimeForHandling']):
            self.mMaxTimeForHandling = int(float(jobDescriptionArr['maxTimeForHandling']))
        
        if ('location' in jobDescriptionArr and None != jobDescriptionArr['location']):
            self.mLocation = jobDescriptionArr['location']
        
        if ('terrain' in jobDescriptionArr and None != jobDescriptionArr['terrain']):
            self.mTerrain = jobDescriptionArr['terrain']
        
        if ('vehicle' in jobDescriptionArr and None != jobDescriptionArr['vehicle']):
            self.mVehicle = jobDescriptionArr['vehicle']
        
        if ('timeOfTheDay' in jobDescriptionArr and None != jobDescriptionArr['timeOfTheDay']):
            self.mTimeOfTheDay = jobDescriptionArr['timeOfTheDay']
        
        if ('weather' in jobDescriptionArr and None != jobDescriptionArr['weather']):
            self.mWeather = jobDescriptionArr['weather']
        
        if ('scenarioType' in jobDescriptionArr and None != jobDescriptionArr['scenarioType']):
            self.mScenarioType = jobDescriptionArr['scenarioType']
        

        self.mDedicatedToOperatorIndex = -1

        if ('dedicatedToOperatorIndex' in jobDescriptionArr):
            self.mDedicatedToOperatorIndex = int(jobDescriptionArr['dedicatedToOperatorIndex'])


        self.mScenarioParams = {}

        scenarioIntParamsFields = Job.getScenarioIntParamsFields()

        for paramName in scenarioIntParamsFields:
            if (paramName in jobDescriptionArr):
                self.mScenarioParams[paramName] = int(float(jobDescriptionArr[paramName]))

        scenarioParamsFields = Job.getScenarioParamsFields()

        for paramName in scenarioParamsFields:
            if (paramName in jobDescriptionArr):
                self.mScenarioParams[paramName] = jobDescriptionArr[paramName]

    @staticmethod
    def toArray(job):
        entityJson = {}

        entityJson['jobId'] = job.getId()
        entityJson['name'] = job.getName()
        entityJson['repetative'] = job.getRepetative()
        entityJson['multiplier'] = job.getMultiplier()
        entityJson['arrivalTime'] = job.getArrivalTime()
        entityJson['maxStartDelay'] = job.getMaxStartDelay()
        entityJson['waitingTime'] = job.getWaitingTime()
        entityJson['activeTime'] = job.getActiveTime()
        entityJson['completedTime'] = job.getCompletedTime()
        entityJson['urgency'] = job.getUrgency()
        entityJson['minTimeForHandling'] = job.getMinTimeForHandling()
        entityJson['maxTimeForHandling'] = job.getMaxTimeForHandling()
        entityJson['location'] = job.getLocation()
        entityJson['terrain'] = job.getTerrain()
        entityJson['vehicle'] = job.getVehicle()
        entityJson['timeOfTheDay'] = job.getTimeOfTheDay()
        entityJson['weather'] = job.getWeather()
        entityJson['scenarioType'] = job.getScenarioType()

        entityJson['scenarioParams'] = job.getScenarioParams()
        
        if (job.getDedicatedOperatorIndex() >= 0):
            entityJson['dedicatedToOperatorIndex'] = job.getDedicatedOperatorIndex()

        return entityJson

    @staticmethod
    def toJson(job):
        return json.dumps(Job.toArray(job))

    def getActiveTime(self):
        return self.mActiveTime

    def onTick(self, currentTime=-1):

        if (self.isAssigned() and self.isWorkStarted(currentTime)):
            self.mActiveTime += 1

        if not self.isAssigned() and currentTime > self.getArrivalTime():
            self.mWaitingTime += 1
            

    def getAssignedTime(self):
        return self.mAssignedTime

    def getFetchedTime(self):
        return self.mFetchedTime

    def getWorkStartedTime(self):
        return self.mWorkStartedTime

    def setAssignedTime(self, assignedTime):
        self.mAssignedTime = assignedTime

    def setFetchedTime(self, fetchedTime):
        self.mFetchedTime = fetchedTime

    def setWorkStartedTime(self, workStartedTime):
        self.mWorkStartedTime = workStartedTime



    def isFetched(self):
        return '' != self.mIsFetched

    def isWorkStarted(self, currentTime):
        return currentTime >= self.mWorkStartedTime

    def isAssigned(self):
        return '' != self.mIsAssigned

    def getAssignedWorkerId(self):
        return self.mIsAssigned

    def setIsAssigned(self, worker_id, currentTime):
        self.mIsAssigned = worker_id
        
        # Disabled
        # self.mActiveTime += 1

        self.setAssignedTime(currentTime)

    def setCompleted(self, currentTime):
        self.mCompletedTime = currentTime

        self.mIsAssigned = ''

    def setRandomRealTime(self, random_real_time):
        self.mRandomRealTime = random_real_time

    def getRandomRealTime(self):
        if self.mRandomRealTime:
            return self.mRandomRealTime
        else:
            print("no random real time sampled for job " + str(self.mId), file=self.f)

    def postponeArrival(self, postponeBy = 1):
        self.mArrivalTime += postponeBy

    def dot(self, dot):
        if self.isAssigned():
            dot.node(str(self.mId), str(self.mId), color='red', group='g1', fontsize='10')
        else:
            dot.node(str(self.mId), str(self.mId), color='lightgrey', style='filled', group='g1', fontsize='10')

    @staticmethod
    def getScenarioIntParamsFields():

        scenarioParamsFields = []

        scenarioParamsFields.append('foreignObjectNumber')
        scenarioParamsFields.append('staticVehicleNumber')
        scenarioParamsFields.append('normalVehicleNumber')
        scenarioParamsFields.append('slowVehicleNumber')
        scenarioParamsFields.append('pedestriansRunningNumber')
        scenarioParamsFields.append('pedestriansCrossingNumber')

        return scenarioParamsFields

    @staticmethod
    def getScenarioParamsFields():

        scenarioParamsFields = []

        scenarioParamsFields.append('townId')
        scenarioParamsFields.append('cloudiness')
        scenarioParamsFields.append('precipitation')
        scenarioParamsFields.append('precipitation_deposits')
        scenarioParamsFields.append('wind_intensity')
        scenarioParamsFields.append('sun_azimuth_angle')
        scenarioParamsFields.append('sun_altitude_angle')
        scenarioParamsFields.append('fog_density')
        scenarioParamsFields.append('fog_distance')
        scenarioParamsFields.append('fog_falloff')
        scenarioParamsFields.append('wetness')
        scenarioParamsFields.append('spawnPointX')
        scenarioParamsFields.append('spawnPointY')
        scenarioParamsFields.append('spawnPointZ')
        scenarioParamsFields.append('spawnPointYaw')
        scenarioParamsFields.append('goalPointX')
        scenarioParamsFields.append('goalPointY')
        scenarioParamsFields.append('goalReachedTolerance')

        return scenarioParamsFields
