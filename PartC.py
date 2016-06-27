from mrjob.job import MRJob
from mrjob.protocol import RawProtocol

#2a
#Write map and reduce functions to compute the distinct years for which there are more
#than 100 temperature observations.

class PartTwo(MRJob):

  def mapper(self, _, tuple):
    row = tuple.split(", ")[6]
    for year in row.split(", "):
      yield (year, 1)

  def reducer(self, year, year_list):
    result = []
    seen = []
    for y in year_list:
      result.append(y)
    if(len(result) > 100):
      yield(year, len(result))


if __name__ == '__main__':
    PartTwo.run()