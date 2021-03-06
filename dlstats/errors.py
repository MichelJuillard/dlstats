
class DlstatsException(Exception):
    
    def __init__(self, *args, **kwargs):
        self.provider_name = kwargs.pop("provider_name", None)
        self.dataset_code = kwargs.pop("dataset_code", None)
        super().__init__(*args, **kwargs)

class RejectFrequency(DlstatsException):

    def __init__(self, *args, **kwargs):
        self.frequency = kwargs.pop("frequency", None)
        super().__init__(*args, **kwargs)
        
class RejectEmptySeries(DlstatsException):
    pass
        

