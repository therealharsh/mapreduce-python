from mrjob.job import MRJob

#2b
#Write map and reduce functions to compute the average temperature for each continent in
#each year, as long as there are more than 50 temperature observations for that continent
#and year.

class PartThree(MRJob):

  def mapper(self, _, tuple):
    year = tuple.split(", ")[6]
    temp = tuple.split(", ")[7]
    continentrow = tuple.split(", ")[3]

    for continent in continentrow.split(", "):
      yield ((continent, year), temp)

  def reducer(self, key, vlist):
    result = []
    allTemp = 0
    for v in vlist:
      result.append(v)
      allTemp = allTemp + float(v)
    if(len(result) > 50):
      avg = allTemp/len(result)
      yield(key, avg)


if __name__ == '__main__':
    PartThree.run()

