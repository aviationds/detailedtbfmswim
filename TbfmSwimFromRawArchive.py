##@Copyright 2020. Codey Wrightbrothers
##@License - This work is licensed under MIT open source license 
###https://opensource.org/licenses/MIT


### Imports
from getopt import getopt
from traceback import format_exc
from datetime import datetime
from sys import exit as sexit,argv
from os import getenv
from bz2 import open as bzopen
from zipfile import ZipFile
from io import TextIOWrapper
from glob import glob
from TbfmSwimParser import TbfmSwimParser
import os

### Main Class
class TbfmSwim:

## Init is used to populate initial values (helpful without command lines)
    def __init__(self):
        #self.inFile = 'tbfm-swim_20200130_2200.xml.bz2'                          # specify .bz2 input files with -i
        self.inFile = 'tbfm-swim_20191101_0700.xml.bz2_filtered.xml.zip'                          # specify .bz2 input files with -i
        self.tracon = 'D01'                      # specify tracon with -t
        self.dest = 'DEN'                        # specify destination airport with -d
        self.artcc = 'ZDV'                       # specify artcc with -a
        self.singleFile=False
        self.fileExt = self.inFile[-3:]
        self.workDir = str(getenv('PWD'))
        self.process_swim_data()
        self.mode = 'test'
        self.filelist = ['']
        self.outputdir = './'
        self.targetdate = ''

##Helper method to obtain timestamp        
    def get_timestamp(self):
        ts = None
        try:
            ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        except Exception:
            print('Get timestamp command raised exception:')
            print(format_exc())
            sexit(1)
        return(ts)    

### Process Command Line Options
    def process_command_line(self):
        print('Begin process_command_line() at ' + self.get_timestamp())
        optlist = None
        args = None
        try:
            optlist, args = getopt(argv[1:],'a:d:e:i:t:m:r:o:z:h:?',['help','h','?'])
        except Exception as e:
            print(str(e))
            self.usage()
        options = dict(optlist)

        if len(args) > 1:
            self.usage()
        
        #print(str(optlist))
        if (len(optlist) < 1):
            self.usage()
        else:
            print(options)
            if ([elem for elem in options if elem in ['-h','--h','-?','--?','--help']]):
                print('Help:')
                self.usage()
            if (not('-m' in options)):
                print("\nError:User must specify a mode with -m\n")
                self.usage()
            else:
                ##We have a mode
                thismode=options['-m']
                if (thismode == 'test'):
                    pass
                elif (thismode =='archive'):
                    print("Archive mode enabled")
                    ###In archive mode, we need -t, -r, -o
                    if (not('-z' in options and '-r' in options and '-o' in options)):
                        print("\nError: User must specify -z, -r, -o with archive mode.\n")
                        self.usage()
                    else:
                        #print("Received all required parameters for archive mode\n")
                        self.targetdate=options['-z']
                        self.outputdir=options['-o']
                        self.filelist=self.getFiles(options['-r'])
                        return
                else:
                    print("Unrecognized mode "+thismode)
                    self.usage()
            if ('-a' in options):
                self.artcc = options['-a']
            if ('-d' in options):
                self.dest = options['-d']
            if ('-i' in options):
                self.singleFile = True
                self.inFile = options['-i']
                self.fileExt = self.inFile[-3:]
            if ('-t' in options):
                self.tracon = options['-t']
            
            if ('-e' in options):
                self.fileExt = options['-e']
                self.singleFile = False
                self.inFile = ''
                flist = sorted(glob('tbfm-swim*.' + self.fileExt))
                #print(flist)
                if (len(flist) == 0):
                    print('No SWIM files found')
                    sexit(1)
                else:
                    for f in flist:
                        self.inFile += f + ' '
                    self.inFile = self.inFile.strip()
        print('End process_command_line() at ' + self.get_timestamp())

    ### The getFiles method takes one or more input directories along with the
    ### assumption of files from 0700 through 0600 and populates a list of files with full path
    ### for processing. If multiple directories are used, they must be
    ### separated with a comma (20191031,20191101).
    ### An assumption is the first set of files 0700 - 2300 is in the first 
    ### directory specified, with the 2nd set of files 0000 - 0600 is in 2nd
    def getFiles(self,dirlist):
        filelist=[]

        dirs=dirlist.split(',')
        ldir1=os.listdir(dirs[0])
        ldir2=os.listdir(dirs[1])
        
        ## Look for files from first directory specified by user    
        for hour in range(7,24):
            hour_str=str('{:02d}'.format(hour))
            file=hour_str+"00"

            for filename in ldir1:
                if (file in  filename):
                    thisdir=dirs[0]
                    filelist.append(thisdir + '/' + filename)

        ## Now same thing for 6 files from next day
        for hour in range(0,7):
            hour_str=str('{:02d}'.format(hour))
            file=hour_str+"00"
            for filename in ldir2:
                if (file in  filename):
                    #print(filename)
                    thisdir=dirs[1]
                    filelist.append(thisdir + '/' + filename)
    
        list_len=len(filelist)

        if (list_len < 23):
            print("Missing a file.")
            print(filelist)

        
        return filelist
        
### Usage
    def usage(self):
        print('\n')
        print('Please use the following command syntax:')
        print('  TbfmSwim.py <-a artcc> <-d dest> <-t tracon> <-i inFile> <-e zip> <-mode test>\n')
        print('  where:')
        print('    -a         specifies the artcc to be used')
        print('    -e         specifies the file extension of the input file(s) to be used')
        print('    -t         specifies the tracon group to be used')
        print('    -d         specifies the destination airport')
        print('    -i         specifies a single input file to be used')
        print('    -h         specifies that help is needed\n')
        print('    -m         Required: specifies test:local or archive:nasa-ntx\n')
        print('    -r         In archive mode, specifies the input directory\n')
        print('    -o         In archive mode, specifies the output directory\n')
        print('    -z         In archive mode, specifies the target date - 20191101\n')        
        print('  Examples:')
        print('    TbfmSwim.py -i tbfm-swim_20200130_2200.xml.bz2\n')
        print('    TbfmSwim.py -e bz2\n')
        sexit(1)

### Gather message data
    def run_message_processing(self):
        print('Begin message_processing() at ' + self.get_timestamp())
        try:
            tsp = TbfmSwimParser()
            tsp.tracon = self.tracon
            tsp.dest = self.dest
            tsp.artcc = self.artcc
            tsp.targetdate=self.targetdate
            tsp.outdir=self.outputdir
            self.fileExt='bz2'
            tsp.write_headers()
            encoding = 'ISO-8859-1'
            ns = {'urn': 'urn:us:gov:dot:faa:atm:tfm:tbfmmeteringpublication'}
            #for fName in self.inFile.split():
            ##Breaking original local test with above
            for fName in self.filelist:
                print('Begin for loop on ' + fName + ' input file at ' + self.get_timestamp())
                if (self.fileExt == 'bz2'):
                    tfil = bzopen(fName,'rt',encoding=encoding)
                    for line in tfil:
                        line = line.strip()
                        if (len(line) == 0):
                            continue
                        if (line[0:4] == '<!--'):
                            continue
                        if (line[-3:] == '-->'):
                            continue
                        else:
                            tsp.process_line(line,ns)
                elif (self.fileExt == 'zip'):
                    with ZipFile(fName) as zfile:
                        for name in zfile.namelist():
                            with zfile.open(name) as readfile:
                                for line in TextIOWrapper(readfile, encoding):
                                    line = line[2:]
                                    line = line[:-4].strip()
                                    if (len(line) == 0):
                                        continue
                                    if (line[0:4] == '<!--'):
                                        continue
                                    if (line[-3:] == '-->'):
                                        continue
                                    else:
                                        tsp.process_line(line,ns)
                print('End for loop on ' + fName + ' input file at ' + self.get_timestamp())
                if (self.fileExt == 'bz2'):
                    tfil.close()
        except:
            print('Running message processing raised exception:')
            print(format_exc())
            sexit(1)
        print('End message_processing() at ' + self.get_timestamp())

### Main process flow
    def process_swim_data(self):
        self.process_command_line()
        self.run_message_processing()
        
##Entry point into this python script, calls main method    
if __name__ == '__main__':
    tbsw = TbfmSwim()
    
