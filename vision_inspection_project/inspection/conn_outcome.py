"""A complete single-frame success takes priority over confirmed range defects."""
class ConnOutcome:
    def __init__(self):
        self.first_ok = None
        self.last_ng = None
        self.confirmed = False

    def observe(self, number, failures, pending, snapshot):
        if failures or pending:
            self.last_ng = (number, snapshot)
        elif self.first_ok is None:
            self.first_ok = (number, snapshot)
        self.confirmed = self.confirmed or bool(failures)

    def result(self):
        if self.first_ok is not None:
            return "OK", self.first_ok
        return "NG", self.last_ng
