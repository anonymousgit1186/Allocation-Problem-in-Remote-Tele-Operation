import json
from xml.dom import minidom

class SettingsManager:

    def __init__(self):

        self.mSettingsList = {}

    def getSettingByName(self, lSettingName):
        
        if (lSettingName not in self.mSettingsList):
            return None
            
        return self.mSettingsList[lSettingName]
