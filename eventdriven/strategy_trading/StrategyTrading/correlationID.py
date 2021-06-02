

class CorrelationID():
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return self.msg

    def get_timestamp(self):
        return int(self.msg.split('|')[0])