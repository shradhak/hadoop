import subprocess
from subprocess import Popen, PIPE
import os
import sys
import shutil
from shutil import copyfile
import tempfile

HOLD_DIR = os.path.dirname(__file__)

HADOOP_CMD_PATH = '/usr/local/hadoop/bin/hadoop'
HADOOP_STREAM_PATH = '/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar'
OUTPUT_PATH = '/user/hduser/temp2'
INPUT_PATH_FILE = '/user/hduser/input1/quakes.csv'

def createArg(mapperArg, reducerArg):
    arg = []
    mArg = ''
    rArg = ''

    for mapArg in mapperArg:
        mArg = mArg + ' ' + mapArg
        
    for redArg in reducerArg:
        rArg = rArg + ' ' + redArg

    arg.append(HADOOP_CMD_PATH)
    arg.append('jar')
    arg.append(HADOOP_STREAM_PATH)
    arg.append('-D')
    arg.append('mapred.map.tasks=3')
    arg.append('-mapper')
    arg.append('"python ./mapper1.py'+ mArg + '"')
    arg.append('-reducer')
    arg.append('"python ./reducer1.py'+ rArg + '"')
    arg.append('-input')
    arg.append('"'+ INPUT_PATH_FILE + '"')
    arg.append('-output')
    arg.append('"' + OUTPUT_PATH + '"')
    
    return arg

def createTmpDir(uname):
    TEMP = tempfile.gettempdir()
    
    reqDir = os.path.join(TEMP, uname)
    
    if not os.path.exists(reqDir):
        os.makedirs(reqDir)
#     os.chmod(reqDir, 777)
    return reqDir

def copyToLoc(fileName, DesDir):
    src = os.path.join(os.path.dirname(__file__), fileName)
    des = os.path.join(DesDir, fileName)
    copyfile(src,des)
    print "Copy from " + src + " to " + des

def main():
    mapperArg = []
    reducerArg = []
    uname = ''
    for i in range(len(sys.argv)):
        if i == 0:
            continue
        if i == 1:
            uname = sys.argv[i]
            uname = uname + '_cloud'
        else:
            mapperArg.append(sys.argv[i])
    if not uname:
        uname = 'brinda_cloud'
    
    Uthres = raw_input("Enter Threshold:")    
#Lthres = raw_input("Enter Lower Threshold:")
#    limit = raw_input("Enter limit:")
    
    mapperArg.append(Uthres)

    
    wrkDir = createTmpDir(uname)
    copyToLoc('mapper1.py', wrkDir)
    copyToLoc('reducer1.py', wrkDir)
   
    os.chdir(wrkDir)
    print "Current Wrk Dir : ", os.getcwd()
    runArg = createArg(mapperArg, reducerArg)
    print runArg
    resultFile = os.path.join(wrkDir, 'result.tsv')
    print resultFile
    # changeToHdUser()
#     output = subprocess.check_output([HADOOP_CMD_PATH, 'fs', '-rm',  OUTPUT_PATH + '/*'], stderr=subprocess.STDOUT)
#     output = subprocess.check_output([HADOOP_CMD_PATH, 'fs', '-rmdir',  OUTPUT_PATH], stderr=subprocess.STDOUT)
    file = open(resultFile, 'w')
    file.write('date\mag\n')
    output = subprocess.check_output(runArg, stderr=subprocess.STDOUT)
    print "Execute Output:\n" + output
    output = subprocess.check_output([HADOOP_CMD_PATH, 'fs', '-cat', OUTPUT_PATH + '/*'], stderr=subprocess.STDOUT)
    file.write(output)
    file.close()
    print "\n\nResult:\n"+ output
    output = subprocess.check_output([HADOOP_CMD_PATH, 'fs', '-rm',  OUTPUT_PATH + '/*'], stderr=subprocess.STDOUT)
    output = subprocess.check_output([HADOOP_CMD_PATH, 'fs', '-rmdir',  OUTPUT_PATH], stderr=subprocess.STDOUT)


if __name__ == '__main__':
    main()
