import json
import sys,getopt
import datetime
import csv
def main(argv):
    print("Running main() with arg {0}".format(argv[0]))
    with open(argv[0],"r") as fin:
        total = 0
        total_gs = 0
        total_idc = 0
        total_phy = 0
        total_unknown = 0
        for row in csv.reader(fin):
            if "Error" in row[0]:
                continue
            if float(row[0].split('_')[-1]) <= 1491321600:
                continue
            if row[6].startswith("R")|row[6].startswith("0")|row[6].startswith("1")|row[6].startswith("S")|row[6].startswith("T"):
                total_phy += float(row[4])/1024/1024/1024
            elif row[6].startswith("DD") | row[6].startswith("DG") | row[6].startswith("DJ"):
                total_gs += float(row[4])/1024/1024/1024
            elif row[6].startswith("DN") | row[6].startswith("DE") | row[6].startswith("DC") | row[6].startswith("CT")|row[6].startswith("DF")| row[6].startswith("DH")| row[6].startswith("DI"):
                total_idc  += float(row[4])/1024/1024/1024
            else:
                print(row)
                total_unknown += float(row[4])/1024/1024/1024
            total += float(row[4])/1024/1024/1024

        print("Physical total size: {0:.2f} TB".format(total_phy))
        print("GS VTL total size: {0:.2f} TB".format(total_gs))
        print("IDC VTL total size: {0:.2f} TB".format(total_idc))
        print("Unkown total size: {0:.2f} TB".format(total_unknown))
        print("Total size: {0:.2f} TB".format(total))


if __name__ == "__main__":
    main(sys.argv[1:])
