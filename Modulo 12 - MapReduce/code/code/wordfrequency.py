from mrjob.job import MRJob
from mrjob.protocol import BytesValueProtocol

class WordFrequency(MRJob):
    INPUT_PROTOCOL = BytesValueProtocol

    def mapper(self, _, line):
        words = line.decode('utf_8').split()

        for word in words:
            yield word.lower(), 1

    def reducer(self, word, values):
       yield word, sum(values)

if __name__ == "__main__":
    WordFrequency.run()
