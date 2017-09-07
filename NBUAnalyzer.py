"""This is the main module"""
import sys,getopt,os
import json
import csv
import time
import datetime as dt
import calendar
import copy
import pprint
from bokeh.plotting import figure, show, output_file
from bokeh.models import HoverTool, ColumnDataSource
import random
import calendar
import collections
import numpy as np
import pandas as pd
import time

#from nbumedia.nbumediaanalyzer import analyzemedia
#from nbuimage.nbuimageanalyzer import extractimages
#from nbuimage.nbuimageanalyzer import analyzeimages_rcp
#from nbuimage.nbuimageanalyzer import analyzeimages_cal
#from nbuimage.chart import showsamplechart

def json_object_iterator(json_filename=""):
    with open(str(json_filename),"r") as json_object_file:
        ret_json_str = ""
        json_str = ""
        for line in json_object_file:
            json_str += line
            if line.startswith("}"):
                ret_json_str = json_str[:]
                json_str = ""
                yield ret_json_str

def extractimages(originaljsonfilename):
    with open(originaljsonfilename + "_csvnbuimages","w") as nbuimagescsv:
        for i,line in enumerate(json_object_iterator(originaljsonfilename)):
            d = json.loads(line)
            backuptime = dt.datetime.fromtimestamp(int(d['backup_time'])).strftime('%Y-%m-%d %H:%M:%S')
            try:
                tapelist = ""
                mediaserver = ""
                if d['frags'] != []:
                    mediaserver = d['frags'][0]['host']
                    for frag in d['frags']:
                        if frag['id'] in tapelist:
                            continue
                        if tapelist == "":
                            tapelist = frag['id']
                        else:
                            tapelist = tapelist + ":" + frag['id']
                nbuimagescsv.write("{0},{1},{2},{3},{4},{5},{6},{7}\n".format(d['backupid'],d['client_name'],d['policy_name'],d['sched_label'],backuptime,d['kbytes'],mediaserver,tapelist))
                if i % 1000 == 0:
                    print("{0} lines processed".format(i))
            except Exception as e:
                print("Error at {0}".format(d['backupid']))
                print(e)
                exc_type, _, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
        print("All processed")

def CreateBackupChart(csvnbuimages):

    with open(csvnbuimages,"r") as fin:
        colormap = ["#444444", "#a6cee3", "#1f78b4", "#b2df8a", "#33a02c", "#fb9a99",
                    "#e31a1c", "#fdbf6f", "#ff7f00"]
        xname = []
        yname = []
        #color = []
        alpha = []
        allmonthdates = []
        allclients = []
        client_job_status_dict = {}
        status = []
        jobpolicy = []
        jobschedule = []
        jobcount = []
        jobsize = []
        hover_client = []
        hover_date = []

        start = dt.datetime(2017,7,1,16,0,0)
        days = calendar.monthrange(2017,7)[1]

        df = pd.read_csv(csvnbuimages,header=None,usecols=[0,1,2,3,4,5,6,7],names = ['backupid','clientname','policyname','schedulename','backupdate','kbytes','mediaserver','tapes'])
        #df = df.head(225)
        df = df.dropna()

        lower = [x.split(".")[0].lower() for x in list(pd.unique(df.clientname.ravel()))]
        df = df.assign(shortname = [x.split(".")[0].lower() for x in list(df.clientname.ravel())])
        df = df.assign(chartdate = [x.strftime('%m-%d') for x in pd.to_datetime(df.backupdate)])

        allclients = list(reversed(sorted(set(lower))))

        #table = collections.OrderedDict()

        for day in range(1,days+1):
            allmonthdates.append(start.strftime("%m-%d"))
            start += dt.timedelta(days=1)

        p = figure(title="Caltex Backup Report",
                x_axis_location="above",tools="hover,save",
                x_range=allmonthdates, y_range=allclients,logo=None)

        p.plot_width = 1200
        p.plot_height = 7200
        p.grid.grid_line_color = None
        p.axis.axis_line_color = None
        p.axis.major_tick_line_color = None
        p.axis.major_label_text_font_size = "9pt"
        p.axis.major_label_standoff = 0
        p.xaxis.major_label_orientation = np.pi/3

        p.rect('chartdate','shortname',0.9, 0.9, source=df,
            color='#3D9140', alpha=0.9, line_color=None,
            hover_line_color='black', hover_color='#228B22')

        p.select_one(HoverTool).tooltips = [
            ('client', '@shortname'),
            ('date', '@chartdate'),
            ('policyname', '@policyname'),
            ('schedulename', '@schedulename'),
            ('kbytes', '@kbytes'),
        ]

        output_file("les_mis.html", title="Backup Report Example")

        show(p) # show the plot

def main():
    """This is the main function"""
    #analyzemedia(os.getcwd() + "\\NetBackup\\nbumedia\\available_media.csv")
    #extractimages(os.getcwd() + "\\NetBackup\\bpimagelist_all_json_cxdappp23_Aug")    
    #extractimages(os.getcwd() + "\\NetBackup\\bpimagelist_all_json")    
    #analyzeimages_rcp(os.getcwd() + "\\NetBackup\\nbuimage\\bpimagelist_all_json" + "_csvnbuimages" )
    #analyzeimages_cal(os.getcwd() + "\\NetBackup\\nbuimage\\bpimagelist_all_json_cxdappp23" + "_csvnbuimages" )
    #showsamplechart()

    try:
        CreateBackupChart(os.getcwd() + "\\NetBackup\\bpimagelist_all_json_cxdappp23_July" + "_csvnbuimages" )
    except Exception as e:
        print(e)
        exc_type, _, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)       
    return

if __name__ == "__main__":
    main()
