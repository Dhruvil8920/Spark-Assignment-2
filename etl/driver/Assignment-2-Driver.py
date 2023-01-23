from etl.test.test import *
# Reading textfile
ReadText().collect()
# Total lines
print(TotalLines())
# Total warnings
print(TotalWarnings())
# Total Api
print(TotalRepository())
# Most HTTP req
MostHttpReq().show()
# Most failed req
failedRequest().show()
# Most active day
ActiveHour().show()
# Most active repo
MostActiveRepo().show()