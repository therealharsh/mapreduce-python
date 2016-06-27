from mrjob.job import MRJob
from mrjob.protocol import RawProtocol

#1a
#Write map and reduce functions to compute an inverted index.

class PartOne(MRJob):
  INPUT_PROTOCOL = RawProtocol

  def mapper(self, url, doc):
    for word in doc.split():
      yield (word, url)

  def reducer(self, word, urls):
    seen = []
    result = []
    for url in urls:
      if url not in seen:
        seen.append(url)
        result.append(url)
    
    yield(word, result)


if __name__ == '__main__':
    PartOne.run()