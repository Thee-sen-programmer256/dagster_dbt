import datetime
with open("./push_pull_logs.txt",'W+') as f:
    f.write("Pulled or pushed at {}".format(datetime.datetime.today()))