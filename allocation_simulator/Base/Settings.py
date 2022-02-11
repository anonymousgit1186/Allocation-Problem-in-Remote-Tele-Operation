import json

class Settings:

    def __init__(self):
        
        self.mDurationSec = 0

    @staticmethod
    def toArray(settings):
        
        entityJson = {}

        entityJson['durationSec'] = settings.getDurationSec()

        return entityJson

    @staticmethod
    def toJson(settings):
        
        return json.dumps(Worker.toArray(settings))
    
    def getDurationSec(self):
        return self.mDurationSec