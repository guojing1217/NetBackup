import sys,getopt
import datetime
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
def main(argv):
#    print("Running main() with arg {}".format(argv[0]))
    for i,line in enumerate(json_object_iterator(argv[0])):
        d = json.loads(line)
        backuptime = datetime.datetime.fromtimestamp(int(d['backup_time'])).strftime('%Y-%m-%d %H:%M:%S')
        try:
            tapelist = ""
            for frag in d['frags']:
                if frag['id'] in tapelist:
                    continue
                if tapelist == "":
                    tapelist = frag['id']
                else:
                    tapelist = tapelist + ":" + frag['id']
            print("{0},{1},{2},{3},{4},{5},{6},{7}".format(d['backupid'],d['client_name'],d['policy_name'],d['sched_label'],backuptime,d['kbytes'],d['frags'][0]['host'],tapelist))
        except Exception as e:
            print("Error at {0}".format(d['backupid']))
#        print("Processing json object {} with backupid {} at {}".format(i,d['backupid'],datetime.datetime.now()))
if __name__ == "__main__":
    main(sys.argv[1:])
