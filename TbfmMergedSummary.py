##@Copyright 2020. Codey Wrightbrothers
##@License - This work is licensed under MIT open source license 
###https://opensource.org/licenses/MIT

import os
import argparse
import os.path
import time
from datetime import datetime 
from datetime import timedelta

##If delays are greater than the following, they are marked with lower confidence
filter_delay_gt_value=90.0
  
def main():

    ##The following are data structures used to house unique flight data
    tbfmFlights,tbfmMF,aarStore,sscStore={},{},{},{}
    aarLookup,sscLookup={},{}
    flt_header,eta_header,sta_header=[],[],[]
    
    ##Read in the files associated with target date
    td=parms["target_date"]
    flat_dir=os.listdir(parms["flattenedDir"])
    flight_dir=os.listdir(parms["fltDir"])
    out_dir=parms["outdir"] 

    ##This returns the files that we will integrated into the summary view
    eta_file,sta_file,flt_file,sch_file,mrp_file,aar_file,ssc_file=getFileNames(flat_dir,flight_dir,td)
    
    ##Expects the flight file. We only care about unique flights in this.
    print("Reading in flight file...")
    full_path=parms["fltDir"] + "/" + flt_file
    f = open(full_path, "r")
    for line in f:
        line=line.rstrip()
        llist=line.split(",")
        if (llist[0] == "aid"):
            ##Header row. Store this for reference
            flt_header=llist
            #print(flt_header)
        else:
            ##Process the contents of this message
            ## start by getting unique key
            aid=llist[0]
            tid=llist[1]
            dap=llist[2]
            apt=llist[3]
            key=aid+"."+tid+"."+dap+"."+apt
            #print(key)
            store={}
            if (key in tbfmFlights):    
                value=tbfmFlights.get(key)
            else:
                tbfmFlights.update({key:store})
                value=tbfmFlights.get(key)
            ##Look for non-null entries
            for i in range(4, len(llist)):
                ##Do nothing with blank entries
                if (not(llist[i]=='')):
                        thisvalue=llist[i]
                        value.update({flt_header[i]:thisvalue})
                        tbfmFlights.update({key:value})

    ### We are reading in the STA first, after flight data, for a reason.
    ## This process helps us remove the intra-center duplicate flights.
    ##  A large percentage of flights have the same meter fix/point within
    ##  The same Center. Analysis of these indicates many of the meter fix
    ##  are never truly used by facilities, but an STA is generated for them
    ##  mainly to keep TBFM up-to-date. This analysis indicates that the 
    ##  flight we really care about is generally the one whose STA comes in
    ##  first. Thus, we process the STAs to see which mfx we should be 
    ##  updating for the remaining data that will be stiched with flight data.
    print("Reading in STA file...")
    full_path=parms["flattenedDir"] + "/" + sta_file
    f = open(full_path, "r")
    for line in f:
        line=line.rstrip()
        llist=line.split(",")
        if (llist[0] == "mti"):
            ##Header row. Store this for reference
            sta_header=llist
            #print(sta_header)
        else:
            ##Process the contents of this message
            ## start by getting unique key
            mti=llist[0]
            artcc=llist[1]
            aid=llist[2]
            tid=llist[3]
            dap=llist[5]
            apt=llist[6]
            mfx=llist[7]
            key=aid+"."+tid+"."+dap+"."+apt
            #print(key)
            mfkey=key+"."+mfx
            
            ##STA messages can come in without any STAs, lets check
            sta_o4a=llist[8]
            sta_o3a=llist[9]	
            sta_ooa=llist[10]	
            sta_oma=llist[11]	
            sta_dfx=llist[12]	
            sta_sfx=llist[13]	
            sta_rwy=llist[14]
            if (sta_o4a=="" and sta_o3a=="" and sta_ooa==""
                and sta_oma=="" and sta_dfx=="" and sta_sfx=="" 
                and sta_rwy==""):
                ##No payload, skip
                continue
            
            
            ##We only care about flights that we have flt data for
            if (not (key in tbfmFlights)):    
                continue

            ##If we have already added this to tbfmMF, get the latest
            if(mfkey in tbfmMF):
                mfvalue=tbfmMF.get(mfkey)
                ##Do nothing with blank entries
                for i in range(7, len(llist)): 
                    if (not(llist[i]=='')):
                        thisvalue=llist[i]
                        mfvalue.update({sta_header[i]:thisvalue})
                        tbfmMF.update({mfkey:mfvalue})        
            
            else:
                ##Let's check to see if this flight already has an STA that
                ## Has come in for a different mfx. If so, we ignore this
                ## STA. If no STA has been recieved, mark this as the first
                ## and add this to the tbfmFlights dict
                
                ##Initialize this MF key w/flight data and get the value dictionary
                mfvalue={}
                value=tbfmFlights.get(key)
                
                first_sta_mfx_tbfmFlights=value.get('mfx_of_first_sta',"")
                if (first_sta_mfx_tbfmFlights == ""): #This is our first, add it 
                    
                    mfvalue.update({'mfx_of_first_sta':mfx})
                    mfvalue.update({'time_at_first_sta':mti})
                    mfvalue.update({'artcc_of_first_sta':artcc})
                    
                    ##Add this info to tbfmFlights in additon to tbfmMF
                    value.update({'mfx_of_first_sta':mfx})
                    tbfmFlights.update({key:value})
                    
                    ##Check to see if this first STA time comes before departed estimate
                    departed_time=value.get('departed',"")
                    if (departed_time == ""):
                        mfvalue.update({'sta_before_departed_boolean':"UNKNOWN"})
                    else:
                        is_earlier=time_greater_by(departed_time,mti,1)
                        if (is_earlier):
                           mfvalue.update({'sta_before_departed_boolean':"1"}) 
                        else:
                           mfvalue.update({'sta_before_departed_boolean':"0"}) 
                    
                    
                    ##Since we are initialzing a new tbfmMF entry, we need flt data
                    ##The following initializes this unique flight without soft copy dict {}
                    for k,v in value.items():
                        mfvalue.update({k:v})
        
                    ##Now add all the non-null elements
                    for i in range(7, len(llist)): 
                        if (not(llist[i]=='')):
                            thisvalue=llist[i]
                            mfvalue.update({sta_header[i]:thisvalue})
                            tbfmMF.update({mfkey:mfvalue})    

    key=""
    print("Reading in MRP file...")
    full_path=parms["flattenedDir"] + "/" + mrp_file
    f = open(full_path, "r")
    for line in f:
        line=line.rstrip()
        llist=line.split(",")
        if (llist[0] == "mti"):
            ##Header row. Store this for reference
            mrp_header=llist
            #print(mrp_header)
            continue
              
        ##Process the contents of this message
        ## start by getting unique key[0]
        mti=llist[0]
        aid=llist[2]
        tid=llist[3]
        dap=llist[5]
        apt=llist[6]
        tra=llist[7]
        mfx=llist[9]
        #scn=llist[19]
        key=aid+"."+tid+"."+dap+"."+apt
        mfkey=key + "."+mfx
        #print(mfkey)
        value={}
        mfvalue={}
            
        ##Let's make for sure this is one of the flights we care about
        if (not (key in tbfmFlights)):    
            continue
            
        ##If we have already added this to tbfmMF, get the latest
        if(mfkey in tbfmMF):
            ##get latest mfvalue dictionary from tbfmMF
            mfvalue=tbfmMF.get(mfkey)
            ##Do nothing with blank entries
            for i in range(7, len(llist)): 
                if (not(llist[i]=='')):
                    thisvalue=llist[i]
                    
                    ##The following is used to update some values right before scheduling
                    last_scheduled=mfvalue.get('time_last_std',"")
                    if (not(last_scheduled=="")) and (not(time_greater_by(last_scheduled,mti,1))):
                        if (mrp_header[i] == "rwy"):
                            mfvalue.update({'rwy_at_last_scheduled':llist[i]})
                        if (mrp_header[i] == "cfg"):
                            mfvalue.update({'cfg_at_last_scheduled':llist[i]})                        
                    
                    mfvalue.update({mrp_header[i]:thisvalue})
                    tbfmMF.update({mfkey:mfvalue}) 

   
    print("Reading in ETA file...")
    full_path=parms["flattenedDir"] + "/" + eta_file
    f = open(full_path, "r")
    for line in f:
        line=line.rstrip()
        llist=line.split(",")
        if (llist[0] == "mti"):
            ##Header row. Store this for reference
            eta_header=llist
            #print(eta_header)
        else:
            ##Process the contents of this message
            ## start by getting unique key
            aid=llist[2]
            tid=llist[3]
            dap=llist[5]
            apt=llist[6]
            mfx=llist[7]
            key=aid+"."+tid+"."+dap+"."+apt
            mfkey=key+"."+mfx
            if (not (key in tbfmFlights)):    
                continue

            ##If we have already added this to tbfmMF, get the latest
            if(mfkey in tbfmMF):
                mfvalue=tbfmMF.get(mfkey)
                ##Do nothing with blank entries
                for i in range(7, len(llist)): 
                    if (not(llist[i]=='')):
                        thisvalue=llist[i]
                        
                        ##The following is used to update some values right before scheduling
                        last_scheduled=mfvalue.get('time_last_std',"")                      
                        if (not(last_scheduled=="")) and (not(time_greater_by(last_scheduled,mti,1))):
                            if (eta_header[i] == "eta_mfx"):
                                mfvalue.update({'mfx_eta_at_last_scheduled':llist[i]})
                            if (eta_header[i] == "eta_rwy"):
                                mfvalue.update({'rwy_eta_at_last_scheduled':llist[i]}) 
                            
                        mfvalue.update({eta_header[i]:thisvalue})
                        tbfmMF.update({mfkey:mfvalue}) 
                
   
    print("Reading in SCH file...")
    full_path=parms["flattenedDir"] + "/" + sch_file
    f = open(full_path, "r")
    for line in f:
        line=line.rstrip()
        llist=line.split(",")
        if (llist[0] == "mti"):
            ##Header row. Store this for reference
            sch_header=llist
            #print(sch_header)
        else:
            ##Process the contents of this message
            ## start by getting unique key
            aid=llist[2]
            tid=llist[3]
            dap=llist[5]
            apt=llist[6]
            mfx=llist[7]
            key=aid+"."+tid+"."+dap+"."+apt
            #print(key)
            mfkey=key+"."+mfx
            #print(mfkey)
            
            if (not (key in tbfmFlights)):    
                continue
            
            ##If we have already added this to tbfmMF, get the latest
            if(mfkey in tbfmMF):
                mfvalue=tbfmMF.get(mfkey)
                ##Do nothing with blank entries
                for i in range(7, len(llist)): 
                    if (not(llist[i]=='')):
                        thisvalue=llist[i]
                        mfvalue.update({sch_header[i]:thisvalue})
                        tbfmMF.update({mfkey:mfvalue})                        
     
    print("Reading in AAR CON file...")
    full_path=parms["flattenedDir"] + "/" + aar_file
    f = open(full_path, "r")
    for line in f:
        line=line.rstrip()
        llist=line.split(",")
        if (llist[0] == "mti"):
            ##Header row. Store this for reference
            aar_header=llist
            #print(aar_header)
        else:
            ##Process the contents of this message
            ## start by getting unique key
            #mti=llist[0]
            #artcc=llist[1]
            tra=llist[3]
            apt=llist[4]
            tim=llist[5]
            rat=llist[6]
            
            ##There are a couple/few parsing errors with bad times
            if (len(tim) > 21):
                #print("bad time "+tim)
                continue

            key=tra+"_"+apt+"_"+tim
            aarStore[key]=rat

    ##The following reformats the previous data into a lookup               
    for k,v in aarStore.items():
       #print(k,v)
       tra,apt,tim=k.split("_")
       rat=v      
       key=tra+"."+apt
       #print('time is '+tim)
       if key in aarLookup:
                aarLookup[key].append(tim)
       else:
                aarLookup[key]=[tim]

    ##The following applies the aar lookup to appropriate flights (arrivals)
    for k,v in tbfmMF.items():
        rat=""
        debug=0
        aid,tid,dap,apt,mfx=k.split(".")
        timesch=v.get('time_last_std',"")
        if (timesch ==""):
            continue
        tra=v.get('tra',"")
        if (tra == "EDC"):
            continue
        else:
            key=tra + "." + apt
            if (key in aarLookup):
              ##Get array of times
              times=aarLookup.get(key)
              if (len(times) ==1):
                  tim=times[0]
                  newkey=tra+"_"+apt+"_"+tim
                  rat=aarStore.get(newkey)
                  if (rat):
                      v.update({"rate":rat})
                      tbfmMF.update({k:v})
                  #print(rat)   
              else:
                  ##Multiple times
                  bestTime=getBestTimeNF(timesch,times,debug)
                  newkey=tra+"_"+apt+"_"+bestTime
                  rat=aarStore.get(newkey)
                  if (rat):
                      v.update({"rate":rat})
                      tbfmMF.update({k:v})                
                  #print(rat)


    print("Reading in SSC CON file...")
    full_path=parms["flattenedDir"] + "/" + ssc_file
    f = open(full_path, "r")
    for line in f:
        line=line.rstrip()
        llist=line.split(",")
        if (llist[0] == "mti"):
            ##Header row. Store this for reference
            ssc_header=llist
            #print(ssc_header)
        else:
            ##Process the contents of this message
            ## start by getting unique key
            if(len(llist)<13):
                continue
            tra=llist[3]
            tim=llist[4]
            scname=llist[12]
            scmre=llist[13]
            ssd=llist[8]
            key=scmre+"."+scname+"."+tim
            #print(key)
            sscStore[key]=ssd
    
    ##The following reformats the previous data into a lookup               
    for k,v in sscStore.items():
       #print(k,v)
       mfx,scname,tim=k.split(".")
       ssd=v      
       key=mfx+"."+scname
       #print(key + " being stored")
       if key in sscLookup:
                sscLookup[key].append(tim)
       else:
                sscLookup[key]=[tim]  
                
    ##The following applies the ssc lookup to flights with matching ssc name
    for k,v in tbfmMF.items():
        rat=""
        debug=0
        aid,tid,dap,apt,mfx=k.split(".")
        timesch=v.get('time_last_std',"")
        if (timesch ==""):
            continue
        mfx=v.get('mfx',"")
        scn=v.get('scn',"")
        key=mfx + "." + scn
        #print(key)
        if (key in sscLookup):
          ##Get array of times
          times=sscLookup.get(key)
          if (len(times) ==1):
                  tim=times[0]
                  newkey=mfx+"."+scn+"."+tim
                  #print(newkey)
                  mit=sscStore.get(newkey,"")
                  v.update({"ssd":mit})
                  tbfmMF.update({k:v})
                  #print(mit)   
          else:
                  ##Multiple times
                  #print(timesch)
                  #print(times)
                  
                  bestTime=getBestTimeNF(timesch,times,debug)
                  newkey=mfx+"."+scn+"."+bestTime
                  #print(newkey)
                  mit=sscStore.get(newkey,"")
                  v.update({"ssd":mit})
                  tbfmMF.update({k:v})  
                  #print(mit)
                  
    ###Mark duplicates 
    dup_flight_plan={}
    dup_flight_listing={}
    for k,v in tbfmMF.items():
        aid,tid,dap,apt,mfx=k.split(".")
        ##remove mfx from unique keys
        key=aid+"."+tid+"."+dap+"."+apt

        value=tbfmFlights.get(key)       
        ptime=value.get('ptime',"")
        nkey=aid+"."+dap+"."+apt+"."+ptime
        
        if (nkey in dup_flight_plan):
            ##We have had at least one other flight with this same key
            nvalue=dup_flight_plan.get(nkey)
            prior_dup=tbfmMF.get(nvalue)
            prior_dup.update({"dup_flight_plan":"1"})
            tbfmMF.update({nvalue:prior_dup})
            v.update({"dup_flight_plan":"1"})
            tbfmMF.update({k:v})
            ##Add this to an array that we will work with later
            dup_list=[nvalue,k]
            dup_flight_listing.update({nkey:dup_list})
        else:
            dup_flight_plan.update({nkey:k})

    ##Iterate through all dups and pick the one with the closest meter fix to origin
    for k in dup_flight_listing:
        v=dup_flight_listing.get(k)
        earliest_eta=""
        earliest_key=""
        for flight in v:
            flight_record=tbfmMF.get(flight)
            eta_sfx=flight_record.get('eta_sfx',"")
            if(earliest_eta == ""):
                earliest_eta = eta_sfx
                earliest_key=flight
            else:
                ##Compare this to earliest_eta
                if (eta_sfx == ""):
                  continue  
                is_earlier=time_greater_by_format(earliest_eta,eta_sfx,1)
                if (is_earlier):
                  earliest_eta=eta_sfx
                  earliest_key=flight
        
        ##Delete all but earliest duplicate flight key
        for flight in v:
            if (flight == earliest_key):
                flight_record=tbfmMF.get(flight)
                flight_record.update({"earliest_dup":"1"})
                tbfmMF.update({flight:flight_record})
            else:
                del tbfmMF[flight]
                

    ### Perform a QUALITY check on the estimated values
    ##There's two parts to each APREQ: the timestamp the APREQ was recorded
    ##("I asked for an STD at AA:AAz"), and the value of the APREQ 
    ##("I will be ready to depart at BB:BBz")
    ##Similarly, two parts to each STD:  the timestamp the STD 
    ##was recorded ("You received your STD at XX:XXz"), and the value 
    ##of the STD ("You will depart at YY:YYz")
    ##The comparison for "stale APREQ" here is the value of the APREQ 
    ##to the timestamp of the STD ( XX:XXz - BB:BBz > 1 minute). 
    ##When a request is made, the flight should be asking for a release 
    ##at a future time, or at least at the current time, but not in the past.
    ##In terms of the field names in the processed csv, 
    ##remove flights where (time_last_std - last_ready_value) > 1 

    for k,v in tbfmMF.items():
        aid,tid,dap,apt,mfx=k.split(".")     
        time_last_std=v.get('time_last_std',"")
        last_ready_value=v.get('last_ready_value',"")
        #print(time_last_std)
        #print(last_ready_value)
        if (time_last_std =="" or last_ready_value ==""):
            #print("no std or ready delete: "+k)
            v.update({"delay_trustworthy":"0"})
            tbfmMF.update({k:v})
            continue
        is_earlier=time_greater_by_format(time_last_std,last_ready_value,1)
        if (is_earlier):           
            last_scheduled_value=v.get('last_scheduled_value',"")
            delay_est=time_diff_min(time_last_std,last_scheduled_value)
            #print("delay estimate is: "+delay_est)
            v.update({"delay_trustworthy":"0.5"})
            v.update({"delay_est(m)":delay_est})
            tbfmMF.update({k:v})
        else:
            v.update({"delay_trustworthy":"1"})
            tbfmMF.update({k:v}) 
        
    ### Perform a QUALITY check on the estimated delay values
    ### If the delay is greater than the parameter value, mark it as .5 trust           
    for k,v in tbfmMF.items():
        delay_trust=v.get('delay_trustworthy',"")
        if (delay_trust == ""):
            print("got a null value for delay trust")
        else:
            if (delay_trust == "1"):
                current_delay_str=v.get('delay_est(m)',"")
                current_delay=float(current_delay_str)
                if (current_delay > filter_delay_gt_value):
                    v.update({"delay_trustworthy":"0.5"})
                    tbfmMF.update({k:v})
    ###Double check our work - testing only
    #test_dup={}
    #for k,v in tbfmMF.items():
    #    aid,tid,dap,apt,mfx=k.split(".")
    #    ##remove mfx from unique keys
    #    key=aid+"."+tid+"."+dap+"."+apt

    #    value=tbfmFlights.get(key)       
    #    ptime=value.get('ptime',"")
    #    nkey=aid+"."+dap+"."+apt+"."+ptime
        
    #    if (nkey in test_dup):
            ##We have had at least one other flight with this same key
    #        print("we have another dup")
    #        print(nkey)
    #    else:
    #        test_dup.update({nkey:k})
    
       
    outFileName= out_dir + "/"+td +"_merged_summary.csv"    
    f = open(outFileName, "w")
    reportFlights(f,tbfmMF)
    exit()
                     


def getBestTime(timesch,times):
    sch_time=datetime.strptime(timesch,'%Y-%m-%d %H:%M:%S%z')
    # Convert to Unix timestamp
    sch_ts = time.mktime(sch_time.timetuple())
    
    lowest_diff=100000000000
    highest_negative=-1000000000000
    best_negative_time=-99999999999
    for t in times:
        tmp_time=datetime.strptime(t,'%Y-%m-%dT%H:%M:%SZ')
        tmp_ts = time.mktime(tmp_time.timetuple())
        secs_diff=sch_ts - tmp_ts
        #print("\n")
        #print(sch_time)
        #print(tmp_time)
        #print(secs_diff)
        if((secs_diff < lowest_diff) and (secs_diff >=0)):
            lowest_diff=secs_diff
            best_pos_time=t
        if((secs_diff > highest_negative) and (secs_diff <=0)):
            best_negative_time=secs_diff
            best_neg_time=t

    if (best_negative_time != -99999999999):
        best_time=best_neg_time
    else:
        best_time=best_pos_time

    #print(best_time)
    return(best_time)

def getBestTimeNF(timesch,times,debug):
    sch_time=datetime.strptime(timesch,'%Y-%m-%dT%H:%M:%SZ')
    # Convert to Unix timestamp
    sch_ts = time.mktime(sch_time.timetuple())
    
    if (debug==1):
        print(timesch)
        print(times)
    lowest_diff=100000000000
    highest_negative=-1000000000000
    best_negative_time=-99999999999
    for t in times:
        tmp_time=datetime.strptime(t,'%Y-%m-%dT%H:%M:%SZ')
        tmp_ts = time.mktime(tmp_time.timetuple())
        secs_diff=sch_ts - tmp_ts
        #print("\n")
        if (debug==1):
            print(sch_time)
            print(tmp_time)
            print(secs_diff)
        if((secs_diff < lowest_diff) and (secs_diff >=0)):
            lowest_diff=secs_diff
            best_pos_time=t
        if((secs_diff > highest_negative) and (secs_diff <=0) and
           (secs_diff > best_negative_time)):
            best_negative_time=secs_diff
            best_neg_time=t

    if (lowest_diff < 100000000000):
        best_time=best_pos_time
    elif (best_negative_time != -99999999999):
        best_time=best_neg_time
    else:
        best_time="UNK"
    
    if (debug==1):
        print("best_neg_time:"+best_neg_time)
        print("best_pos_time:"+best_pos_time)
        print("best_tim:e"+best_time)
    #print(best_time)
    return(best_time)


##Helper method to compare two times    
def time_greater_by(now,then,val):
    then_array=then.split(".")
    newNow = datetime.strptime(now,'%Y-%m-%dT%H:%M:%SZ')
    newThen = datetime.strptime(then_array[0],'%Y-%m-%dT%H:%M:%S')
    tDiff = (newNow - newThen) > timedelta(seconds = val)
    return(tDiff)

##Helper method to compare two times    
def time_greater_by_format(now,then,val):
   
    newNow = datetime.strptime(now,'%Y-%m-%dT%H:%M:%SZ')
    newThen =datetime.strptime(then,'%Y-%m-%dT%H:%M:%SZ')
    tDiff = (newNow - newThen) > timedelta(seconds = val)
    return(tDiff)

##Helper method to compare two times    
def time_diff_min(ready,scheduled):
   
    ready_time = datetime.strptime(ready,'%Y-%m-%dT%H:%M:%SZ')
    scheduled_time =datetime.strptime(scheduled,'%Y-%m-%dT%H:%M:%SZ')
    tDiff = scheduled_time - ready_time 

    minutes=tDiff.seconds / 60

    ##If there are even a few seconds difference between the ready and scheduled
    ## the time diff we are doing shows a full day difference. This next
    ## check just sets the estimated delay value to 0 in these cases
    if (minutes >300.0):
        minutes = 0.0   
    min_string=format(round(minutes,2), '.2f')
    
    return(min_string)
       
def reportFlights(f,tbfmMF):
    ##print header
    f.write("key,aid,tid,dap,apt,mfx,ptime,last_time_apreq,last_ready_value,")
    f.write("last_scheduled_value,delay_est(m),edct,departed,last_etd,num_unique_stds,time_last_std,")
    f.write("last_eng,last_spd,last_ara,last_ina,last_trw,last_drw,last_tds,last_cfx,")
    f.write("last_ctm,last_etm,last_est,last_mfx,last_gat,last_eta_o4a,")
    f.write("last_eta_o3a,last_eta_ooa,last_eta_oma,last_eta_dfx,last_eta_sfx,")
    f.write("last_eta_mfx,last_eta_rwy,last_sta_o4a,last_sta_o3a,last_sta_ooa,last_sta_oma,last_sta_dfx,")
    f.write("last_sta_sfx,last_sta_rwy,sfz,sus,man,rfz,last_tra,last_gat,")
    f.write("dfx,sfx,oma,ooa,o3a,o4a,rwy,cfg,cat,scn,arrival_rate_at_last_schedule,")
    f.write("ssd_at_last_schedule,rwy_at_last_scheduled,cfg_at_last_scheduled,")
    f.write("mfx_eta_at_last_scheduled,rwy_eta_at_last_scheduled,last_artcc,")
    f.write("last_a10,mfx_of_first_sta,time_of_first_sta,artcc_of_first_sta,")
    f.write("sta_before_departed_boolean,delay_trustworthy\n")
 
    
    for k,v in tbfmMF.items():
        aid,tid,dap,apt,mfx=k.split(".")
        #key=aid+"."+tid+"."+dap+"."+apt
        #value=tbfmFlights.get(k)
        #dup_flight_plan=v.get("dup_flight_plan","0")
        delay_trustworthy=v.get("delay_trustworthy","")
        
        f.write(k+","+aid+","+tid+","+dap+","+apt +","+mfx+",")
        f.write(v.get('ptime',"")+",")
        f.write(v.get('last_time_apreq',"")+",")
        f.write(v.get('last_ready_value',"")+",")
        f.write(v.get('last_scheduled_value',"")+",")
        f.write(v.get('delay_est(m)',"")+",")
        f.write(v.get('edct',"")+",")
        f.write(v.get('departed',"")+",")
        f.write(v.get('last_etd',"")+",")
        f.write(v.get('num_unique_stds',"")+",")        
        f.write(v.get('time_last_std',"")+",")
        f.write(v.get('last_eng',"")+",")
        f.write(v.get('last_spd',"")+",")
        f.write(v.get('last_ara',"")+",")
        f.write(v.get('last_ina',"")+",")
        f.write(v.get('last_trw',"")+",")
        f.write(v.get('last_drw',"")+",")
        f.write(v.get('last_tds',"")+",")
        f.write(v.get('last_cfx',"")+",")
        f.write(v.get('last_ctm',"")+",")
        f.write(v.get('last_etm',"")+",")
        f.write(v.get('last_est',"")+",")
        f.write(v.get('mfx',"")+",")
        f.write(v.get('gat',"")+",")
        f.write(v.get('eta_o4a',"")+",")
        f.write(v.get('eta_o3a',"")+",")
        f.write(v.get('eta_ooa',"")+",")
        f.write(v.get('eta_oma',"")+",")
        f.write(v.get('eta_dfx',"")+",")
        f.write(v.get('eta_sfx',"")+",")
        f.write(v.get('eta_mfx',"")+",")
        f.write(v.get('eta_rwy',"")+",")
        f.write(v.get('sta_o4a',"")+",")
        f.write(v.get('sta_o3a',"")+",")
        f.write(v.get('sta_ooa',"")+",")
        f.write(v.get('sta_oma',"")+",")
        f.write(v.get('sta_dfx',"")+",")
        f.write(v.get('sta_sfx',"")+",")
        f.write(v.get('sta_rwy',"")+",")
        f.write(v.get('sfz',"")+",")
        f.write(v.get('sus',"")+",")
        f.write(v.get('man',"")+",")
        f.write(v.get('rfz',"")+",")
        f.write(v.get('tra',"")+",")
        f.write(v.get('gat',"")+",")
        f.write(v.get('dfx',"")+",")
        f.write(v.get('sfx',"")+",")
        f.write(v.get('oma',"")+",")
        f.write(v.get('ooa',"")+",")
        f.write(v.get('o3a',"")+",")
        f.write(v.get('o4a',"")+",")
        f.write(v.get('rwy',"")+",")
        f.write(v.get('cfg',"")+",")
        f.write(v.get('cat',"")+",")
        f.write(v.get('scn',"")+",")
        f.write(v.get('rate',"")+",")
        f.write(v.get('ssd',"")+",")
        f.write(v.get('rwy_at_last_scheduled',"")+",")
        f.write(v.get('cfg_at_last_scheduled',"")+",")
        f.write(v.get('mfx_eta_at_last_scheduled',"")+",")
        f.write(v.get('rwy_eta_at_last_scheduled',"")+",")     
        f.write(v.get('last_artcc',"")+",")
        f.write(v.get('last_a10',"")+",")
        #f.write(str(dup_flight_plan)+",")
        f.write(v.get('mfx_of_first_sta',"")+",")
        f.write(v.get('time_at_first_sta',"")+",")
        f.write(v.get('artcc_of_first_sta',"")+",")
        f.write(v.get('sta_before_departed_boolean',"")+",")
        f.write(str(delay_trustworthy))
        f.write("\n")
                        
                        
def getFileNames(flat_dir,flight_dir,td):
    ##ETA flattened file for this day
    aar_file,ssc_file="",""
    for file in flat_dir:
        #print(file)
        if ((file.find(td) != -1) and
            file.endswith("TbfmSwimParserAirEta.csv")):
            eta_file=file
     
    ##STA flattened file for this day
    for file in flat_dir:
        #print(file)
        if ((file.find(td) != -1) and
            file.endswith("TbfmSwimParserAirSta.csv")):
            sta_file=file

    ##flight file for this day
    for file in flight_dir:
        #print(file)
        if ((file.find(td) != -1) and
            file.endswith("TbfmSwimDepDelayEst.csv")):
            flt_file=file
    
    ##SCH flattened file for this day
    for file in flat_dir:
        #print(file)
        if ((file.find(td) != -1) and
            file.endswith("TbfmSwimParserAirSch.csv")):
            sch_file=file

    ##MRP flattened file for this day
    for file in flat_dir:
        #print(file)
        if ((file.find(td) != -1) and
            file.endswith("TbfmSwimParserAirMrp.csv")):
            mrp_file=file

    ##CON AAR flattened file for this day
    for file in flat_dir:
        #print(file)
        if ((file.find(td) != -1) and
            file.endswith("TbfmSwimParserConAar.csv")):
            aar_file=file
            
    ##CON SSC flattened file for this day
    for file in flat_dir:
        #print(file)
        if ((file.find(td) != -1) and
            file.endswith("TbfmSwimParserConSscScl.csv")):
            ssc_file=file            
    
    return(eta_file,sta_file,flt_file,sch_file,mrp_file,aar_file,ssc_file)
    
def build_parms(args):
    """Helper function to parse command line arguments into dictionary
        input: ArgumentParser class object
        output: dictionary of expected parameters
    """
    flattenedDir=args.flattened_dir
    fltDir=args.sch_flt_dir
    #target_date=args.target_date
    target_date=args.target_date
    outdir=args.outdir  
    parms = {"flattenedDir":flattenedDir,
             "fltDir":fltDir,
             "target_date":target_date,
             "outdir":outdir}
    
    return(parms)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description = \
                 "Cleans up flt data from previous processing steps.")
    parser.add_argument("flattened_dir", 
                        help = "Directory to obtain flattended SWIM CSVs.")
    parser.add_argument("sch_flt_dir", 
                        help = "Directory to obtain flt scheduled CSV.")
    parser.add_argument("target_date", 
                        help = "Local Day to Focus on. e.g. 20191101.")
    parser.add_argument("--outdir", default = "./")
    parms = build_parms(parser.parse_args())
    main()

