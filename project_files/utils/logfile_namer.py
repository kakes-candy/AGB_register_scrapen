import datetime
import os.path


def logfileNamer(path, basename):

    # log bestandsnaam
    datum = datetime.datetime.now().strftime("%Y%m%d")
    logfilename = "{datum}_{basename}".format(datum=datum, basename=basename)

    # Loop om bestanden te nummeren als op dezelde dag hetzelfde script gedraaid wordt
    i = 1
    while True:
        # Check of bestand al bestaat op log locatie
        if os.path.exists(os.path.join(path, logfilename + ".log")):
            logfilename = "{datum}_{basename}-{i}".format(
                datum=datum, basename=basename, i=i
            )
            i += 1
            continue
        else:
            logpath = os.path.join(path, logfilename + ".log")
            break

    return logpath
