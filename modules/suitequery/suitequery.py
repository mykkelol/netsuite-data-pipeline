from suitequery import SuiteQuery

class SuiteQuery:
    def __init__(self, filters, columns):
        self.filters = filters
        self.columns = columns
        self._results = []

    def get_results(self):
        return [
            'my results are here!'
        ]