##@Copyright 2020. Codey Wrightbrothers
##@License - This work is licensed under MIT open source license 
###https://opensource.org/licenses/MIT


from pandas import read_csv as pdread_csv
from traceback import format_exc
from datetime import datetime,timedelta
from sys import exit as sexit
import argparse
import os

##The following python dictionaries store key data used during processing
##This is just one efficient way to store flight specific info as we process
statusDict,ctmPTimeDict,ctmReadyDict,ctmApreqDict = {},{},{},{}
ctmScheduledDict,stdScheduledDict,ctmDepartedDict,etmEdctDict = {},{},{},{}
etdEstimatedDict,nextCtmReady,lastReadySet,landedDict = {},{},{},{}
readyReason,drwSet,readySet,readySetAtDeparture = {},{},{},{}
numUniqueStd,stdSetFromEtd,timeStdSet,engSet,tcrSet={},{},{},{},{}
bcnSet,spdSet,araSet,inaSet,trwSet,drwSet,tdsSet={},{},{},{},{},{},{}
cfxSet,ctmSet,etmSet,estSet,a10Set,b10Set,c10Set={},{},{},{},{},{},{}
lastArtccSet,lastAcsSet,lastFpsSet={},{},{}

##Output File suffix
outfile_suffix="_TbfmSwimDepDelayEst.csv"

##The number of TBFM SWIM 'std' messages can be set to 0, but generally 1 is best
##This effectively filters out flights that do not have 'std' messages which
##correspond to the TBFM scheduled time of departure
minimumStds=0

## Main processing loop that calls helper methods as required
def main():

    ##Read in command line options and set initial parameters from them    
    ldir=os.listdir(parms["readDir"])
    path=parms["readDir"]
    out_dir=parms["outdir"]    
    td=parms["target_date"]
    

    ###Winnow down the files to those in input dir that match the target date
    file_to_process=[]
    for file in ldir:
        if ((file.find(td) != -1) and (file.find("AirFlt") != -1)):
            file_to_process.append(file)
    file=path + "/" + file_to_process[0]

    print('Begin reading input file ' +file + ' to dataframe at ' + get_timestamp())
    
    ##Read into a pandas dataframe
    df = pdread_csv(file, index_col=False, parse_dates=False,low_memory=False)
    df.fillna('', inplace=True)
    print('End reading input file '+file+ ' to dataframe at ' + get_timestamp())
    
    print('Begin loop through dataframe at ' + get_timestamp())
    for row in df.itertuples(index=False):
        
        ##Set the latest based upon data available from the CSV file
        mti = row.mti
        artcc = row.artcc
        aid = row.aid
        tid=row.tid
        aty = row.aty
        dap = row.dap
        apt = row.apt
        fps = row.fps
        acs = row.acs
        eng = row.eng
        bcn = row.bcn
        spd = row.spd
        ara = row.ara
        ina = row.ina
        trw = row.trw
        drw = row.drw
        tds = row.tds
        cfx = row.cfx
        ctm = row.ctm
        etd = row.etd
        std = row.std
        etm = row.etm
        est = row.est
        a10 = row.a10
        b10 = row.b10
        c10 = row.c10
        tcr = row.tcr
        
        #apply ready and scheduled time determination algorithm
        #wl1 83/91% on ready, 97/98% on scheduled, 21 missing FAA not SWIM, 208 SWIM not FAA

        ##Somewhat unique key for this TBFM flight information
        key = aid+'_'+tid+'_'+dap+'_'+apt

        #set FP status at the top of this logic. The status then 
        # plays into further checks below
        if (fps != ''):
            ##We may want to check if this just became departed or active here
            statusDict[key] = fps
            
        #set P-Time from ctm if we already don't have one
        if ((fps == 'PROPOSED') & (ctm != '')):
            if (not(key in ctmPTimeDict)):
                ctmPTimeDict[key] = ctm

        #set or remove ready time with ctm, if appropriate
        if ((statusDict.get(key) == 'PROPOSED')):
            if ((aty == 'AMD') & (ctm != '')):
                ##here we already have a ready time, but are getting AMD,
                ## with ctm and drw. Remove prior ready and replace with this one
                if (drw != ''): # or (key in drwSet)):
                    if (key in ctmReadyDict):
                        del(ctmReadyDict[key])
                        del(ctmApreqDict[key])
                #set ready time
                if (not(key in ctmReadyDict)):
                    if ((key in ctmPTimeDict) & (ctmPTimeDict.get(key) != ctm)):
                        ctmReadyDict[key] = ctm
                        ctmApreqDict[key] = reset_mti(mti)
                

            #set scheduled time if std is populated (and while PROPOSED)
            #if we have coordination time, update or remove as appropriate
            if ((std != '')):
                
                ##First, lets see if we already have this same std or rescheduled
                if (key in stdScheduledDict):
                    currentStd=stdScheduledDict[key]
                    if (currentStd == std):
                        if (key in stdSetFromEtd):
                            del(stdSetFromEtd[key])                       
                    else:
                        numStds=numUniqueStd[key] if (key in numUniqueStd) else 0
                        numStds=numStds+1
                        numUniqueStd[key]=numStds
                        stdScheduledDict[key] = std
                        timeStdSet[key]=reset_mti(mti)
                else: 
                   stdScheduledDict[key] = std
                   numUniqueStd[key]=1
                   timeStdSet[key]=reset_mti(mti)
                   
                ##If we have a ctm already, 
                if ((key in ctmReadyDict)):
                    #if ready > scheduled, set ready = scheduled
                    if (time_greater_by(ctmReadyDict.get(key),std,0)):
                        ctmReadyDict[key] = std
                else:
                    if (key in ctmReadyDict):
                        del(ctmReadyDict[key])
                        del(ctmApreqDict[key])
                    if (key in ctmScheduledDict):
                        del(ctmScheduledDict[key])

            #Set ETD Time (and potentially scheduled time)
            if (etd != ''):
                etdEstimatedDict[key] = etd
                #if etd is set, set std = etd
                if (not(key in stdScheduledDict)):
                    stdScheduledDict[key] = etd
                    stdSetFromEtd[key] = 1
                    
        #set Departure Time using ctm at departure. Also sets ready time to
        # std if ctm is missing. 
        if ((fps == 'DEPARTED') & (ctm != '')):
            ctmDepartedDict[key] = ctm
            if ((key in stdScheduledDict) & (not(key in ctmReadyDict))):
                ctmReadyDict[key] = stdScheduledDict.get(key)

        #set departure time if it is not already and we have ctm
        if (fps == 'ACTIVE'):
            if (not(key in ctmDepartedDict)):
                if (ctm != ''):
                    ctmDepartedDict[key] = ctm
                else:
                    ctmDepartedDict[key] = reset_mti(mti)
            if ((key in stdScheduledDict) & (not(key in ctmReadyDict))):
                ctmReadyDict[key] = stdScheduledDict.get(key)
                readySetAtDeparture[key] = 1

        #set EDCT Time
        if (etm != ''):
            etmEdctDict[key] = etm
        
        ##Set a flag that this flight has had at least one drw
        if (drw != ''):
            drwSet[key]=drw
            
        ##Set other things if you like, but need to add Dict for it
        if (eng != ''):
            engSet[key]=eng
        
        if (bcn != ''):
            bcnSet[key]=bcn
        
        if (spd != ''):
            spdSet[key]=spd
        
        if (ara != ''):
            araSet[key]=ara
        
        if (ina != ''):
            inaSet[key]=ina
        
        if (trw != ''):
            trwSet[key]=trw
        
        if (drw != ''):
            drwSet[key]=drw
        
        if (tds != ''):
            tdsSet[key]=tds
        
        if (cfx != ''):
            cfxSet[key]=cfx
        
        if (ctm != ''):
            ctmSet[key]=ctm
       
        if (etm != ''):
            etmSet[key]=etm
        
        if (est != ''):
            estSet[key]=est
        
        if (a10 != ''):
            a10Set[key]=a10
        
        if (b10 != ''):
            b10Set[key]=b10
        
        if (c10 != ''):
            c10Set[key]=c10
        
        if (artcc != ''):
            lastArtccSet[key]=artcc

        if (tcr != ''):
            tcrSet[key]=tcr
            
        if (acs != ''):
            lastAcsSet[key]=acs

        if (fps != ''):
            lastFpsSet[key]=fps            
       
        ##Null out temporary variables before reading the next line
        mti,artcc,aid,tid,aty,dap,apt,fps = '','','','','','','',''
        acs,eng,bcn,spd,ara,ina,trw,drw = '','','','','','','',''
        tds,cfx,ctm,etd,std,etm,est,a10 = '','','','','','','',''
        b10,c10,tcr = '','',''
        
    print('End loop through dataframe at ' + get_timestamp())

    #write data to output CSV file
    print('Begin output of delay data at ' + get_timestamp())
    output_delay_data(out_dir,td)
    print('End output of delay data at ' + get_timestamp())

##Helper method to obtain current timestamp
def get_timestamp():
    ts = None
    try:
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    except Exception:
        print('Get timestamp command raised exception:')
        print(format_exc())
        sexit(1)
    return(ts)    

##Helper method to print the output by looping through each flight and 
## populating the output when the element exists
def output_delay_data(out_dir,td):

    full_out_path=out_dir + "/" +td + outfile_suffix
    outFile = open(full_out_path, 'w')
    outFile.write('aid,tid,dap,apt,ptime,last_time_apreq,last_ready_value,last_scheduled_value,delay_est(m),edct,departed,last_etd,num_unique_stds,time_last_std,last_eng,last_spd,last_ara,last_ina,last_trw,last_drw,last_tds,last_cfx,last_ctm,last_etm,last_est,last_tcr,last_artcc,last_acs,last_fps,last_a10\n')

    ##Loop through each flight that had a scheduled time
    ##This limits the output to only those flights that might have had 
    ##a surface delay passed back from TBFM
    for key,value in stdScheduledDict.items():
        (aid,tid,dap,apt) = key.split('_')
        numStds=numUniqueStd[key] if (key in numUniqueStd) else 0
        ready = ctmReadyDict.get(key) if (key in ctmReadyDict) else ''
        ##The following will filter out flights with no STDs from SWIM
        if ( numStds >= minimumStds or ready ==""):
           
            ptime = ctmPTimeDict.get(key) if (key in ctmPTimeDict) else ''
            apreq = ctmApreqDict.get(key) if (key in ctmApreqDict) else ''
            
            scheduled = value
            edct = etmEdctDict.get(key) if (key in etmEdctDict) else ''
            departed = ctmDepartedDict.get(key) if (key in ctmDepartedDict) else ''
            etd = etdEstimatedDict.get(key) if (key in etdEstimatedDict) else ''
            #rdyrsn = readyReason.get(key) if (key in readyReason) else ''
            #readySetAtDep = readySetAtDeparture[key] if (key in readySetAtDeparture) else 0
            #stdFromEtd = stdSetFromEtd[key] if (key in stdSetFromEtd) else 0
    
            timeLastStdSet=timeStdSet[key] if (key in timeStdSet) else ''
    
            ##Other items we are just keeping track of and others may find useful
            eng=engSet[key] if (key in engSet) else ''
            spd=spdSet[key] if (key in spdSet) else ''
            ara=araSet[key] if (key in araSet) else ''
            ina=inaSet[key] if (key in inaSet) else ''
            twr=trwSet[key] if (key in trwSet) else ''
            drw=drwSet[key] if (key in drwSet) else ''
            tds=tdsSet[key] if (key in tdsSet) else ''
            cfx=cfxSet[key] if (key in cfxSet) else ''
            ctm=ctmSet[key] if (key in ctmSet) else ''
            etm=etmSet[key] if (key in etmSet) else ''
            est=estSet[key] if (key in estSet) else ''
            a10=a10Set[key] if (key in a10Set) else ''
            b10=b10Set[key] if (key in b10Set) else ''
            c10=c10Set[key] if (key in c10Set) else ''
            tcr=tcrSet[key] if (key in tcrSet) else ''
            artcc=lastArtccSet[key] if (key in lastArtccSet) else ''
            acs=lastAcsSet[key] if (key in lastAcsSet) else '' 
            fps=lastFpsSet[key] if (key in lastFpsSet) else '' 
            
            delay_est=getEstDelay(ready,scheduled)
            
            outStr = '{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(aid,
                      tid,dap,apt,ptime,apreq,ready,scheduled,delay_est,edct,departed,etd,
                      numStds,timeLastStdSet,
                      eng,spd,ara,ina,twr,drw,tds,cfx,ctm,etm,est,tcr,artcc,acs,
                      fps,a10)
                      
            outFile.write(outStr + '\n')
    outFile.close()

##Helper method to reset the message time         
def reset_mti(mti):
    tStr = mti[:-5] + 'Z'
    return(tStr)

##Helper method to take calculated a delay estimate
def getEstDelay(ready,scheduled):

    if (ready =="" or scheduled ==""):
        return("UNKNOWN")
        
    ready_time = datetime.strptime(ready[:-1] + '+0000','%Y-%m-%dT%H:%M:%S%z')
    scheduled_time = datetime.strptime(scheduled[:-1] + '+0000','%Y-%m-%dT%H:%M:%S%z')
    tDiff = scheduled_time - ready_time 
    minutes=tDiff.seconds / 60
    min_string=format(round(minutes,2), '.2f')
    #print("{:.2f}".format(round(a, 2)))
    return(min_string)
    
##Helper method to take the time difference between two input strings
def timediff(first,second):
    newNow = datetime.strptime(first[:-1] + '+0000','%Y-%m-%dT%H:%M:%S%z')
    newThen = datetime.strptime(second[:-1] + '+0000','%Y-%m-%dT%H:%M:%S%z')
    tDiff = newThen - newNow 
    return(tDiff)

##Helper method to compare two times    
def time_greater_by(now,then,val):
    newNow = datetime.strptime(now[:-1] + '+0000','%Y-%m-%dT%H:%M:%S%z')
    newThen = datetime.strptime(then[:-1] + '+0000','%Y-%m-%dT%H:%M:%S%z')
    tDiff = (newNow - newThen) > timedelta(minutes = val)
    return(tDiff)

##Helper method to read in command line variables and populate params
def build_parms(args):
    """Helper function to parse command line arguments into dictionary
        input: ArgumentParser class object
        output: dictionary of expected parameters
    """
    readDir=args.dir
    #target_date=args.target_date
    target_date=args.target_date
    outdir=args.outdir  
    parms = {"readDir":readDir,
             "target_date":target_date,
             "outdir":outdir}
    
    return(parms)

##Entry point into this python script, calls main method
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description = \
                 "Devired data from TBFM SWIM flights.")
    parser.add_argument("dir", 
                        help = "Directory to obtain flattened TBFM SWIM.")
    parser.add_argument("target_date", 
                        help = "Local Day to Focus on. e.g. 20191101.")
    parser.add_argument("--outdir", default = "./")
    parms = build_parms(parser.parse_args())
    main()
    
