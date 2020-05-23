##@Copyright 2020. Codey Wrightbrothers
##@License - This work is licensed under MIT open source license 
###https://opensource.org/licenses/MIT

### Imports
from getopt import getopt
from traceback import format_exc
from datetime import datetime
from sys import exit as sexit,argv
from os import getenv
from zipfile import ZipFile
from io import TextIOWrapper
from bz2 import open as bzopen
from xml.etree import ElementTree as ET

### Main Class
class TbfmSwimParser:

    def __init__(self):
        self.inFile = '.bz2'                          # specify .bz2 input files with -i
        self.tracon = 'D01'                       # specify tracon with -t
        self.dest = 'DEN'                         # specify destination airport with -d
        self.artcc = 'ZDV'                        # specify artcc with -a
        self.workDir = str(getenv('PWD'))
        self.outdir = '.'
        self.targetdate = 'test'

        #self.process_swim_data()

    def write_headers(self):
        self.air_flt_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserAirFlt.csv', 'w')
        self.air_flt_file.write('mti,artcc,aid,tid,aty,dap,apt,fps,acs,typ,eng,bcn,spd,ara,ina,trw,drw,tds,cfx,ctm,etd,std,etm,est,old,a10,b10,c10,tcr\n')
        self.air_sta_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserAirSta.csv', 'w')
        self.air_sta_file.write('mti,artcc,aid,tid,aty,dap,apt,mfx,sta_o4a,sta_o3a,sta_ooa,sta_oma,sta_dfx,sta_sfx,sta_rwy\n')
        self.air_eta_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserAirEta.csv', 'w')
        self.air_eta_file.write('mti,artcc,aid,tid,aty,dap,apt,mfx,eta_o4a,eta_o3a,eta_ooa,eta_oma,eta_dfx,eta_sfx,eta_mfx,eta_rwy\n')
        self.air_sch_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserAirSch.csv', 'w')
        self.air_sch_file.write('mti,artcc,aid,tid,aty,dap,apt,mfx,sfz,sus,man,rfz\n')
        self.air_mrp_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserAirMrp.csv', 'w')
        self.air_mrp_file.write('mti,artcc,aid,tid,aty,dap,apt,tra,gat,mfx,dfx,sfx,oma,ooa,o3a,o4a,rwy,cfg,cat,scn\n')
        self.con_aac_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserConAac.csv', 'w')
        self.con_aac_file.write('mti,artcc,aty,tra,apt,tim,cfg\n')
        self.con_aar_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserConAar.csv', 'w')
        self.con_aar_file.write('mti,artcc,aty,tra,apt,tim,rat\n')
        self.con_tar_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserConTar.csv', 'w')
        self.con_tar_file.write('mti,artcc,tty,tra,tim,rat\n')
        self.con_gar_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserConGar.csv', 'w')
        self.con_gar_file.write('mti,artcc,gty,tra,apt,gat,tim,rat\n')
        self.con_mar_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserConMar.csv', 'w')
        self.con_mar_file.write('mti,artcc,mty,tra,apt,mfx,tim,rat\n')
        self.con_rar_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserConRar.csv', 'w')
        self.con_rar_file.write('mti,artcc,rty,tra,apt,rwy,tim,rat\n')
        self.con_ssc_scl_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserConSscScl.csv', 'w')
        self.con_ssc_scl_file.write('mti,artcc,sccty,tra,tim,sscty,ssn,sscname,ssd,ssmin,sstyp,sclty,scname,scmre\n')
        self.con_ssc_cc_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserConSscCc.csv', 'w')
        self.con_ssc_cc_file.write('mti,artcc,sccty,tra,tim,sscty,ssn,sscname,ssd,ssmin,sstyp,ccty,apt,apreq,sch\n')
        self.con_sac_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserConSac.csv', 'w')
        self.con_sac_file.write('mti,artcc,sacty,tra,tim,san,scf\n')
        self.oth_tmg_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserOthTmg.csv', 'w')
        self.oth_tmg_file.write('mti,artcc,tra,tmgty,apt,ctr,mfx,mas\n')
        self.oth_int_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserOthInt.csv', 'w')
        self.oth_int_file.write('mti,artcc,ifn,ift,ifs,ifm\n')
        self.adp_trn_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserAdpTrn.csv', 'w')
        self.adp_trn_file.write('mti,artcc,nam\n')
        self.adp_gans_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserAdpGans.csv', 'w')
        self.adp_gans_file.write('mti,artcc,tra,nam\n')
        self.adp_mrns_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserAdpMrns.csv', 'w')
        self.adp_mrns_file.write('mti,artcc,tra,nam,mrt,gat,mfx,oma,ooa,o3a,o4a,lat,lon,rad,lan,ran,ahi,alo\n')
        self.adp_apns_rwy_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserAdpApnsRwy.csv', 'w')
        self.adp_apns_rwy_file.write('mti,artcc,tra,anam,rwys\n')
        self.adp_apns_cfg_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserAdpApnsCfg.csv', 'w')
        self.adp_apns_cfg_file.write('mti,artcc,tra,anam,cfg,rwys\n')
        self.adp_scns_file = open(self.outdir + "/" +self.targetdate + '_TbfmSwimParserAdpScns.csv', 'w')
        self.adp_scns_file.write('mti,artcc,tra,nam\n')

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
            optlist, args = getopt(argv[1:],'a:d:i:t:o:rh?',['help','h','?'])
        except Exception as e:
            print(str(e))
            self.usage()
        options = dict(optlist)
        if len(args) > 1:
            self.usage()
        else:
            if ([elem for elem in options if elem in ['-h','--h','-?','--?','--help']]):
                print('Help:')
                self.usage()
            if ('-a' in options):
                self.artcc = options['-a']
            if ('-d' in options):
                self.dest = options['-d']
            if ('-i' in options):
                self.inFile = options['-i']
            if ('-t' in options):
                self.tracon = options['-t']
            if ('-o' in options):
                self.outdir = options['-o']
            if ('-r' in options):
                self.targetdate = options['-r']
                
        print('End process_command_line() at ' + self.get_timestamp())

    ### Usage
    def usage(self):
        print('\n')
        print('Please use the following command syntax:')
        print('  TbfmSwimParser.py -a artcc -d dest -t tracon -i inFile\n')
        print('  where:')
        print('    -a         specifies the artcc to be used')
        print('    -t         specifies the tracon group to be used')
        print('    -d         specifies the destination airport')
        print('    -i         specifies a single input file to be used')
        print('    -h         specifies that help is needed\n')
        print('    -t         specifies the target date for output files\n')
        print('    -h         specifies the output directory\n')        
        print('  Examples:')
        print('    TbfmSwimParser.py -a ZDV -d DEN -t D01 -i tbfm-swim_20200130_2200.xml.bz2\n')
        sexit(1)

    def get_iata_from_icao(self,apt):
        tstStr = str(apt)
        if ((len(tstStr) == 4) & ((tstStr[0] == 'K') | (tstStr[0] == 'C'))):
            tstStr = tstStr[1:]
        return(tstStr)

    ### Gather message data
    def run_message_processing(self):
        print('Begin message_processing() at ' + self.get_timestamp())
        try:
            print('Begin for loop on input file at ' + self.get_timestamp())
            fileExt = self.inFile[-3:]
            encoding = 'ISO-8859-1'
            ns = {'urn': 'urn:us:gov:dot:faa:atm:tfm:tbfmmeteringpublication'}
            if (fileExt == 'bz2'):
                tfil = bzopen(self.inFile,'rt',encoding=encoding)
                for line in tfil:
                    line = line.strip()
                    if (len(line) == 0):
                        continue
                    if (line[0:4] == '<!--'):
                        continue
                    if (line[-3:] == '-->'):
                        continue
                    else:
                        self.process_line(line,ns)
            elif (fileExt == 'zip'):
                with ZipFile(self.inFile) as zfile:
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
                                    self.process_line(line,ns)
            print('End for loop on input file at ' + self.get_timestamp())
            tfil.close()
        except:
            print('Running message processing raised exception:')
            print(format_exc())
            sexit(1)
        print('End message_processing() at ' + self.get_timestamp())

    def process_line(self,line,ns):
        root = ET.fromstring(line)
        for tma in root.findall('urn:tma', ns):
            for air in tma.findall('urn:air', ns):
                self.parse_air(root,tma,air,ns,line)
            for con in tma.findall('urn:con', ns):
                self.parse_con(root,tma,con,ns,line)
            for oth in tma.findall('urn:oth', ns):
                self.parse_oth(root,tma,oth,ns,line)
            for adp in tma.findall('urn:adp', ns):
                self.parse_adp(root,tma,adp,ns,line)

    def parse_air(self,root,tma,air,ns,line):
        for flt in air.findall('urn:flt', ns):
            self.parse_flt(root,tma,air,flt,ns,line)
        for sta in air.findall('urn:sta', ns):
            self.parse_sta(root,tma,air,sta,ns,line)
        for eta in air.findall('urn:eta', ns):
            self.parse_eta(root,tma,air,eta,ns,line)
        for mrp in air.findall('urn:mrp', ns):
            self.parse_mrp(root,tma,air,mrp,ns,line)
        for sch in air.findall('urn:sch', ns):
            self.parse_sch(root,tma,air,sch,ns)
        for trk in air.findall('urn:trk', ns):
            self.parse_trk(root,tma,air,trk,ns,line)

    def parse_flt(self,root,tma,air,flt,ns,line):
        #Arrival - UAL401
        #Departure - UAL948
        #Scheduled - LOF4666
#        if ((air.attrib.get('aid') == 'UAL401') |
#            (air.attrib.get('aid') == 'UAL948') |
#            (air.attrib.get('aid') == 'UAL302') |
#            (air.attrib.get('aid') == 'LOF4784') |
#            (air.attrib.get('aid') == 'LOF4666')):
            artcc = root.attrib.get('envSrce')
            artcc = artcc.split('.')[1]
            #fps Flight Plan Status
            fps = self.check_none(flt.findtext('urn:fps', None, ns))
            #acs Aircraft Status
            acs = self.check_none(flt.findtext('urn:acs', None, ns))
            #if ((fps != '') | (acs != '')):
            #mti msgTime
            mti = tma.attrib.get('msgTime')
            #aid Aircraft Identification
            aid = self.check_none(flt.findtext('urn:aid', None, ns))
            #tid tmaId
            tid = air.attrib.get('tmaId')
            #aty airType
            aty = air.attrib.get('airType')
            #dap Departure Airport
            dap = self.check_none(flt.findtext('urn:dap', None, ns))
            dap = self.get_iata_from_icao(dap)
            #apt Destination Airport
            apt = self.check_none(flt.findtext('urn:apt', None, ns))
            apt = self.get_iata_from_icao(apt)
            #typ Aircraft Type
            typ = self.check_none(flt.findtext('urn:typ', None, ns))
            #eng Engine Type
            eng = self.check_none(flt.findtext('urn:eng', None, ns))
            #bcn Beacon Code
            bcn = self.check_none(flt.findtext('urn:bcn', None, ns))
            #spd Filed Speed
            spd = self.check_none(flt.findtext('urn:spd', None, ns))
            #ara Assigned/Requested Altitude
            ara = self.check_none(flt.findtext('urn:ara', None, ns))
            #ina Interim Altitude
            ina = self.check_none(flt.findtext('urn:ina', None, ns))
            #trw TRACON Runway Name
            trw = self.check_none(flt.findtext('urn:trw', None, ns))
            #drw Departure Runway Name
            drw = self.check_none(flt.findtext('urn:drw', None, ns))
            #tds Current Track Data Source
            tds = self.check_none(flt.findtext('urn:tds', None, ns))
            #cfx Coordination Fix Name
            cfx = self.check_none(flt.findtext('urn:cfx', None, ns))
            #ctm Coordination Time
            ctm = self.check_none(flt.findtext('urn:ctm', None, ns))
            #etd Estimated Departure Time
            etd = self.check_none(flt.findtext('urn:etd', None, ns))
            #std Scheduled Departure Time
            std = self.check_none(flt.findtext('urn:std', None, ns))
            #etm EDCT Time
            etm = self.check_none(flt.findtext('urn:etm', None, ns))
            #est EDCT Status
            est = self.check_none(flt.findtext('urn:est', None, ns))
            #old
            old = self.check_none(flt.findtext('urn:old', None, ns))
            #a10 Field 10A Route
            a10 = self.check_none(flt.findtext('urn:a10', None, ns))
            #b10 Field 10B Route
            b10 = self.check_none(flt.findtext('urn:b10', None, ns))
            #c10 Field 10C Route
            c10 = self.check_none(flt.findtext('urn:c10', None, ns))
            #tcr TMA Converted Route
            tcr = self.check_none(flt.findtext('urn:tcr', None, ns))
            outStr = '{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(mti,
                                                          artcc,
                                                          aid,
                                                          tid,
                                                          aty,
                                                          dap,
                                                          apt,
                                                          fps,
                                                          acs,
                                                          typ,
                                                          eng,
                                                          bcn,
                                                          spd,
                                                          ara,
                                                          ina,
                                                          trw,
                                                          drw,
                                                          tds,
                                                          cfx,
                                                          ctm,
                                                          etd,
                                                          std,
                                                          etm,
                                                          est,
                                                          old,
                                                          a10,
                                                          b10,
                                                          c10,
                                                          tcr)
            self.air_flt_file.write(outStr + '\n')
            #add in algorithm for ready time and scheduled time determination

    def parse_sta(self,root,tma,air,sta,ns,line):
                artcc = root.attrib.get('envSrce')
                artcc = artcc.split('.')[1]
#        if (artcc == self.artcc):
#            if ((air.attrib.get('aid') == 'UAL401') |
#                (air.attrib.get('aid') == 'UAL948') |
#                (air.attrib.get('aid') == 'UAL302') |
#                (air.attrib.get('aid') == 'LOF4784') |
#                (air.attrib.get('aid') == 'LOF4666')):
#            if ((air.attrib.get('aid') == 'UAL401')):
                #mti msgTime
                mti = tma.attrib.get('msgTime')
                #aid Aircraft Identification
                aid = air.attrib.get('aid')
                #tid tmaId
                tid = air.attrib.get('tmaId')
                #aty airType
                aty = air.attrib.get('airType')
                #dap Departure Airport
                dap = air.attrib.get('dap')
                #apt Destination Airport
                apt = air.attrib.get('apt')
                #mfx Meter Fix
                mfx = self.check_none(sta.findtext('urn:mfx', None, ns))
                #sta_o4a sta at o4a
                sta_o4a = self.check_none(sta.findtext('urn:sta_o4a', None, ns))
                #sta_o3a sta at o3a
                sta_o3a = self.check_none(sta.findtext('urn:sta_o3a', None, ns))
                #sta_ooa sta at ooa
                sta_ooa = self.check_none(sta.findtext('urn:sta_ooa', None, ns))
                #sta_oma sta at oma
                sta_oma = self.check_none(sta.findtext('urn:sta_oma', None, ns))
                #sta_dfx sta at dfx
                sta_dfx = self.check_none(sta.findtext('urn:sta_dfx', None, ns))
                #sta_sfx sta at sfx
                sta_sfx = self.check_none(sta.findtext('urn:sta_sfx', None, ns))
                #sta_rwy sta at rwy
                sta_rwy = self.check_none(sta.findtext('urn:sta_rwy', None, ns))
                outStr = '{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(mti,
                                                          artcc,
                                                          aid,
                                                          tid,
                                                          aty,
                                                          dap,
                                                          apt,
                                                          mfx,
                                                          sta_o4a,
                                                          sta_o3a,
                                                          sta_ooa,
                                                          sta_oma,
                                                          sta_dfx,
                                                          sta_sfx,
                                                          sta_rwy)
                self.air_sta_file.write(outStr + '\n')
                #print(line + '\n')

    def parse_eta(self,root,tma,air,eta,ns,line):
                artcc = root.attrib.get('envSrce')
                artcc = artcc.split('.')[1]
#        if (artcc == self.artcc):
#            if ((air.attrib.get('aid') == 'UAL401') |
#                (air.attrib.get('aid') == 'UAL948') |
#                (air.attrib.get('aid') == 'UAL302') |
#                (air.attrib.get('aid') == 'LOF4784') |
#                (air.attrib.get('aid') == 'LOF4666')):
#            if ((air.attrib.get('aid') == 'UAL401')):
                #mti msgTime
                mti = tma.attrib.get('msgTime')
                #aid Aircraft Identification
                aid = air.attrib.get('aid')
                #tid tmaId
                tid = air.attrib.get('tmaId')
                #aty airType
                aty = air.attrib.get('airType')
                #dap Departure Airport
                dap = air.attrib.get('dap')
                #apt Destination Airport
                apt = air.attrib.get('apt')
                #mfx Meter Fix
                mfx = self.check_none(eta.findtext('urn:mfx', None, ns))
                #eta_o4a eta at o4a
                eta_o4a = self.check_none(eta.findtext('urn:eta_o4a', None, ns))
                #eta_o3a eta at o3a
                eta_o3a = self.check_none(eta.findtext('urn:eta_o3a', None, ns))
                #eta_ooa eta at ooa
                eta_ooa = self.check_none(eta.findtext('urn:eta_ooa', None, ns))
                #eta_oma eta at oma
                eta_oma = self.check_none(eta.findtext('urn:eta_oma', None, ns))
                #eta_dfx eta at dfx
                eta_dfx = self.check_none(eta.findtext('urn:eta_dfx', None, ns))
                #eta_sfx eta at sfx
                eta_sfx = self.check_none(eta.findtext('urn:eta_sfx', None, ns))
                #eta_mfx
                eta_mfx = self.check_none(eta.findtext('urn:eta_mfx', None, ns))
                #eta_rwy eta at rwy
                eta_rwy = self.check_none(eta.findtext('urn:eta_rwy', None, ns))
                outStr = '{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(mti,
                                                          artcc,
                                                          aid,
                                                          tid,
                                                          aty,
                                                          dap,
                                                          apt,
                                                          mfx,
                                                          eta_o4a,
                                                          eta_o3a,
                                                          eta_ooa,
                                                          eta_oma,
                                                          eta_dfx,
                                                          eta_sfx,
                                                          eta_mfx,
                                                          eta_rwy)
                self.air_eta_file.write(outStr + '\n')
                #print(line + '\n')

    def parse_mrp(self,root,tma,air,mrp,ns,line):
            artcc = root.attrib.get('envSrce')
            artcc = artcc.split('.')[1]
#        if ((air.attrib.get('aid') == 'UAL401') |
#            (air.attrib.get('aid') == 'UAL948') |
#            (air.attrib.get('aid') == 'UAL302') |
#            (air.attrib.get('aid') == 'LOF4784') |
#            (air.attrib.get('aid') == 'LOF4666')):
            #mti msgTime
            mti = tma.attrib.get('msgTime')
            #aid Aircraft Identification
            aid = air.attrib.get('aid')
            #tid tmaId
            tid = air.attrib.get('tmaId')
            #aty airType
            aty = air.attrib.get('airType')
            #dap Departure Airport
            dap = air.attrib.get('dap')
            #apt Destination Airport
            apt = air.attrib.get('apt')
            #tra Tracon
            tra = self.check_none(mrp.findtext('urn:tra', None, ns))
            #gat Gate
            gat = self.check_none(mrp.findtext('urn:gat', None, ns))
            #mfx Meter Fix
            mfx = self.check_none(mrp.findtext('urn:mfx', None, ns))
            #dfx Display Fix
            dfx = self.check_none(mrp.findtext('urn:dfx', None, ns))
            #sfx Scheduling Fix
            sfx = self.check_none(mrp.findtext('urn:sfx', None, ns))
            #oma Outer Meter Arc
            oma = self.check_none(mrp.findtext('urn:oma', None, ns))
            #ooa Outer Outer Arc
            ooa = self.check_none(mrp.findtext('urn:ooa', None, ns))
            #o3a Outer Three Arc
            o3a = self.check_none(mrp.findtext('urn:o3a', None, ns))
            #o4a Outer Four Arc
            o4a = self.check_none(mrp.findtext('urn:o4a', None, ns))
            #rwy Arrival Runway
            rwy = self.check_none(mrp.findtext('urn:rwy', None, ns))
            #cfg Configuration
            cfg = self.check_none(mrp.findtext('urn:cfg', None, ns))
            #cat Category
            cat = self.check_none(mrp.findtext('urn:cat', None, ns))
            #scn Stream Class
            scn = self.check_none(mrp.findtext('urn:scn', None, ns))
            outStr = '{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(mti,artcc,aid,tid,aty,dap,apt,tra,gat,mfx,dfx,sfx,oma,ooa,o3a,o4a,rwy,cfg,cat,scn)
            self.air_mrp_file.write(outStr + '\n')
    
    def parse_sch(self,root,tma,air,sch,ns):
            artcc = root.attrib.get('envSrce')
            artcc = artcc.split('.')[1]
#        if ((air.attrib.get('aid') == 'UAL401') |
#            (air.attrib.get('aid') == 'UAL948') |
#            (air.attrib.get('aid') == 'UAL302') |
#            (air.attrib.get('aid') == 'LOF4784') |
#            (air.attrib.get('aid') == 'LOF4666')):
            #mti msgTime
            mti = tma.attrib.get('msgTime')
            #aid Aircraft Identification
            aid = air.attrib.get('aid')
            #tid tmaId
            tid = air.attrib.get('tmaId')
            #aty airType
            aty = air.attrib.get('airType')
            #dap Departure Airport
            dap = air.attrib.get('dap')
            #apt Destination Airport
            apt = air.attrib.get('apt')
            #mfx Meter Fix
            mfx = self.check_none(sch.findtext('urn:mfx', None, ns))
            #sfz Scheduling Frozen
            sfz = self.check_none(sch.findtext('urn:sfz', None, ns))
            #sus Scheduling Suspended
            sus = self.check_none(sch.findtext('urn:sus', None, ns))
            #man Manually Scheduled
            man = self.check_none(sch.findtext('urn:man', None, ns))
            #rfz Runway Frozen
            rfz = self.check_none(sch.findtext('urn:rfz', None, ns))
            outStr = '{},{},{},{},{},{},{},{},{},{},{},{}'.format(mti,artcc,aid,tid,aty,dap,apt,mfx,sfz,sus,man,rfz)
            self.air_sch_file.write(outStr + '\n')

    def parse_trk(self,root,tma,air,trk,ns,line):
        print(line + '\n')
    
    def parse_con(self,root,tma,con,ns,line):
        for aac in con.findall('urn:aac', ns):
            self.parse_aac(root,tma,con,aac,ns,line)
        for aar in con.findall('urn:aar', ns):
            self.parse_aar(root,tma,con,aar,ns)
        for tar in con.findall('urn:tar', ns):
            self.parse_tar(root,tma,con,tar,ns)
        for gar in con.findall('urn:gar', ns):
            self.parse_gar(root,tma,con,gar,ns)
        for mar in con.findall('urn:mar', ns):
            self.parse_mar(root,tma,con,mar,ns)
        for rar in con.findall('urn:rar', ns):
            self.parse_rar(root,tma,con,rar,ns)
        for scc in con.findall('urn:scc', ns):
            self.parse_scc(root,tma,con,scc,ns,line)
        for sac in con.findall('urn:sac', ns):
            self.parse_sac(root,tma,con,sac,ns,line)

    def parse_aac(self,root,tma,con,aac,ns,line):
            artcc = root.attrib.get('envSrce')
            artcc = artcc.split('.')[1]
            tra = self.check_none(aac.findtext('urn:tra', None, ns))
            apt = self.check_none(aac.findtext('urn:apt', None, ns))
#        if ((tra == self.tracon) & (apt == self.dest)):
            #mti msgTime
            mti = tma.attrib.get('msgTime')
            #aty aacType
            aty = aac.attrib.get('aacType')
            #tim Configuration Change Time
            tim = self.check_none(aac.findtext('urn:tim', None, ns))
            #cfg Configuration
            cfg = self.check_none(aac.findtext('urn:cfg', None, ns))
            outStr = '{},{},{},{},{},{},{}'.format(mti,
                                                artcc,
                                                aty,
                                                tra,
                                                apt,
                                                tim,
                                                cfg)
            self.con_aac_file.write(outStr + '\n')
            #print(line + '\n')

    def parse_aar(self,root,tma,con,aar,ns):
            artcc = root.attrib.get('envSrce')
            artcc = artcc.split('.')[1]
            tra = self.check_none(aar.findtext('urn:tra', None, ns))
            apt = self.check_none(aar.findtext('urn:apt', None, ns))
#        if ((tra == self.tracon) & (apt == self.dest)):
            #mti msgTime
            mti = tma.attrib.get('msgTime')
            #aty aarType
            aty = aar.attrib.get('aarType')
            #tim Acceptance Rate Change Time
            tim = self.check_none(aar.findtext('urn:tim', None, ns))
            #rat Acceptance Rate
            rat = self.check_none(aar.findtext('urn:rat', None, ns))
            outStr = '{},{},{},{},{},{},{}'.format(mti,
                                                artcc,
                                                aty,
                                                tra,
                                                apt,
                                                tim,
                                                rat)
            self.con_aar_file.write(outStr + '\n')
        
    def parse_tar(self,root,tma,con,tar,ns):
            artcc = root.attrib.get('envSrce')
            artcc = artcc.split('.')[1]
            tra = self.check_none(tar.findtext('urn:tra', None, ns))
#        if ((tra == self.tracon)):
            #mti msgTime
            mti = tma.attrib.get('msgTime')
            #aty aarType
            tty = tar.attrib.get('tarType')
            #tim Acceptance Rate Change Time
            tim = self.check_none(tar.findtext('urn:tim', None, ns))
            #rat Acceptance Rate
            rat = self.check_none(tar.findtext('urn:rat', None, ns))
            outStr = '{},{},{},{},{},{}'.format(mti,
                                                artcc,
                                                tty,
                                                tra,
                                                tim,
                                                rat)
            self.con_tar_file.write(outStr + '\n')
        
    def parse_gar(self,root,tma,con,gar,ns):
            artcc = root.attrib.get('envSrce')
            artcc = artcc.split('.')[1]
            tra = self.check_none(gar.findtext('urn:tra', None, ns))
            apt = self.check_none(gar.findtext('urn:apt', None, ns))
#        if ((tra == self.tracon) & (apt == self.dest)):
            #mti msgTime
            mti = tma.attrib.get('msgTime')
            #aty aarType
            gty = gar.attrib.get('garType')
            #rwy Gate
            gat = self.check_none(gar.findtext('urn:gat', None, ns))
            #tim Gate Acceptance Rate Change Time
            tim = self.check_none(gar.findtext('urn:tim', None, ns))
            #rat Gate Acceptance Rate
            rat = self.check_none(gar.findtext('urn:rat', None, ns))
            outStr = '{},{},{},{},{},{},{},{}'.format(mti,
                                                artcc,
                                                gty,
                                                tra,
                                                apt,
                                                gat,
                                                tim,
                                                rat)
            self.con_gar_file.write(outStr + '\n')
        
    def parse_mar(self,root,tma,con,mar,ns):
            artcc = root.attrib.get('envSrce')
            artcc = artcc.split('.')[1]
            tra = self.check_none(mar.findtext('urn:tra', None, ns))
            apt = self.check_none(mar.findtext('urn:apt', None, ns))
#        if ((tra == self.tracon) & (apt == self.dest)):
            #mti msgTime
            mti = tma.attrib.get('msgTime')
            #aty aarType
            mty = mar.attrib.get('marType')
            #rwy Meter Fix
            mfx = self.check_none(mar.findtext('urn:mfx', None, ns))
            #tim Meter Fix Acceptance Rate Change Time
            tim = self.check_none(mar.findtext('urn:tim', None, ns))
            #rat Meter Fix Acceptance Rate
            rat = self.check_none(mar.findtext('urn:rat', None, ns))
            outStr = '{},{},{},{},{},{},{},{}'.format(mti,
                                                artcc,
                                                mty,
                                                tra,
                                                apt,
                                                mfx,
                                                tim,
                                                rat)
            self.con_mar_file.write(outStr + '\n')
        
    def parse_rar(self,root,tma,con,rar,ns):
            artcc = root.attrib.get('envSrce')
            artcc = artcc.split('.')[1]
            tra = self.check_none(rar.findtext('urn:tra', None, ns))
            apt = self.check_none(rar.findtext('urn:apt', None, ns))
#        if ((tra == self.tracon) & (apt == self.dest)):
            #mti msgTime
            mti = tma.attrib.get('msgTime')
            #aty aarType
            rty = rar.attrib.get('rarType')
            #rwy Runway
            rwy = self.check_none(rar.findtext('urn:rwy', None, ns))
            #tim Runway Acceptance Rate Change Time
            tim = self.check_none(rar.findtext('urn:tim', None, ns))
            #rat Runway Acceptance Rate
            rat = self.check_none(rar.findtext('urn:rat', None, ns))
            outStr = '{},{},{},{},{},{},{},{}'.format(mti,
                                                artcc,
                                                rty,
                                                tra,
                                                apt,
                                                rwy,
                                                tim,
                                                rat)
            self.con_rar_file.write(outStr + '\n')
        
    def parse_scc(self,root,tma,con,scc,ns,line):
                artcc = root.attrib.get('envSrce')
                artcc = artcc.split('.')[1]
#        if (artcc == self.artcc):
                tra = self.check_none(scc.findtext('urn:tra', None, ns))
#            if ((tra == self.tracon) | (tra == 'EDC')):
                #mti msgTime
                mti = tma.attrib.get('msgTime')
                #scty sccType
                sccty = scc.attrib.get('sccType')
                #tim Super Stream Class (SSC) Flow Change Time
                tim = self.check_none(scc.findtext('urn:tim', None, ns))
                #for sta in air.findall('urn:sta', ns):
                for sscs in scc.findall('urn:sscs', ns):
                    for ssc in sscs.findall('urn:ssc', ns):
                        #sscty sscType attrib
                        sscty = ssc.attrib.get('sscType')
                        #ssn SSC Number
                        ssn = self.check_none(ssc.findtext('urn:ssn', None, ns))
                        #sscname SSC
                        sscname = self.check_none(ssc.findtext('urn:sscname', None, ns))
                        #ssd SSC Separation Distance
                        ssd = self.check_none(ssc.findtext('urn:ssd', None, ns))
                        #ssmin SSC Minutes In Trail
                        ssmin = self.check_none(ssc.findtext('urn:ssmin', None, ns))
                        #sstyp SSC Separation Type
                        sstyp = self.check_none(ssc.findtext('urn:sstyp', None, ns))
                        for scls in ssc.findall('urn:scls', ns):
                            for scl in scls.findall('urn:scl', ns):
                                #sclty sclType attrib
                                sclty = scl.attrib.get('sclType')
                                #scname Stream Class
                                scname = self.check_none(scl.findtext('urn:scname', None, ns))
                                #scmre Stream Class Meter Reference Element
                                scmre = self.check_none(scl.findtext('urn:scmre', None, ns))
                                outStr = '{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(mti,artcc,sccty,tra,tim,sscty,ssn,sscname,ssd,ssmin,sstyp,sclty,scname,scmre)
                                self.con_ssc_scl_file.write(outStr + '\n')
                        for ccs in ssc.findall('urn:ccs',ns):
                            for cc in ccs.findall('urn:cc',ns):
                                #ccty ccType attrib
                                ccty = cc.attrib.get('ccType')
                                #apt Airport 
                                apt = self.check_none(cc.findtext('urn:apt', None, ns))
                                #apreq Apreq Status
                                apreq = self.check_none(cc.findtext('urn:apreq', None, ns))
                                #sch Scheduling Mode
                                sch = self.check_none(cc.findtext('urn:sch', None, ns))
                                outStr = '{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(mti,artcc,sccty,tra,tim,sscty,ssn,sscname,ssd,ssmin,sstyp,ccty,apt,apreq,sch)
                                self.con_ssc_cc_file.write(outStr + '\n')
        
    def parse_sac(self,root,tma,con,sac,ns,line):
                artcc = root.attrib.get('envSrce')
                artcc = artcc.split('.')[1]
#        if (artcc == self.artcc):
                tra = self.check_none(sac.findtext('urn:tra', None, ns))
#            if ((tra == self.tracon) | (tra == 'EDC')):
                #mti msgTime
                mti = tma.attrib.get('msgTime')
                #saty sacType
                sacty = sac.attrib.get('sacType')
                #tim Satellite Airport Configuration Change Time
                tim = self.check_none(sac.findtext('urn:tim', None, ns))
                for saps in sac.findall('urn:saps', ns):
                    for sap in saps.findall('urn:sap', ns):
                        #san
                        san = self.check_none(sap.findtext('urn:san', None, ns))
                        #scf
                        scf = self.check_none(sap.findtext('urn:scf', None, ns))
                        #mti,sacty,tra,tim,san,scf
                        outStr = '{},{},{},{},{},{},{}'.format(mti,artcc,sacty,tra,tim,san,scf)
                        self.con_sac_file.write(outStr + '\n')
        
    def parse_oth(self,root,tma,oth,ns,line):
        for tmg in oth.findall('urn:tmg', ns):
            self.parse_tmg(root,tma,oth,tmg,ns,line)
        for int in oth.findall('urn:int', ns):
            self.parse_int(root,tma,oth,int,ns,line)

    def parse_tmg(self,root,tma,oth,tmg,ns,line):
                artcc = root.attrib.get('envSrce')
                artcc = artcc.split('.')[1]
#        if (artcc == self.artcc):
                tra = self.check_none(tmg.findtext('urn:tra', None, ns))
#            if ((tra == self.tracon) | (tra == 'EDC')):
                #mti msgTime
                mti = tma.attrib.get('msgTime')
                #tmgty tmgType
                tmgty = tmg.attrib.get('tmgType')
                for tmss in tmg.findall('urn:tmss', ns):
                    for tms in tmss.findall('urn:tms', ns):
                        #apt Metered Airport
                        apt = self.check_none(tms.findtext('urn:apt', None, ns))
                        #ctr Center
                        ctr = self.check_none(tms.findtext('urn:ctr', None, ns))
                        #mfx Meter Point
                        mfx = self.check_none(tms.findtext('urn:mfx', None, ns))
                        #mas Metering Active Status
                        mas = self.check_none(tms.findtext('urn:mas', None, ns))
                        outStr = '{},{},{},{},{},{},{},{}'.format(mti,artcc,tra,tmgty,apt,ctr,mfx,mas)
                        self.oth_tmg_file.write(outStr + '\n')

    def parse_int(self,root,tma,oth,int,ns,line):
            artcc = root.attrib.get('envSrce')
            artcc = artcc.split('.')[1]
#        if (artcc == self.artcc):
            #mti msgTime
            mti = tma.attrib.get('msgTime')
            #ifn Interface
            ifn = self.check_none(int.findtext('urn:ifn', None, ns))
            #ift Interface Type
            ift = self.check_none(int.findtext('urn:ift', None, ns))
            #ifs Interface Status
            ifs = self.check_none(int.findtext('urn:ifs', None, ns))
            #ifm Interface Metering Status
            ifm = self.check_none(int.findtext('urn:ifm', None, ns))
            outStr = '{},{},{},{},{},{}'.format(mti,artcc,ifn,ift,ifs,ifm)
            self.oth_int_file.write(outStr + '\n')

    def parse_adp(self,root,tma,adp,ns,line):
        for trn in adp.findall('urn:trn', ns):
            self.parse_trn(root,tma,adp,trn,ns,line)
        for gans in adp.findall('urn:gans', ns):
            self.parse_gans(root,tma,adp,gans,ns,line)
        for mrns in adp.findall('urn:mrns', ns):
            self.parse_mrns(root,tma,adp,mrns,ns,line)
        for scns in adp.findall('urn:scns', ns):
            self.parse_scns(root,tma,adp,scns,ns,line)
        for apns in adp.findall('urn:apns', ns):
            self.parse_apns(root,tma,adp,apns,ns,line)

    def parse_trn(self,root,tma,adp,trn,ns,line):
            artcc = root.attrib.get('envSrce')
            artcc = artcc.split('.')[1]
#        if (artcc == self.artcc):
            #mti msgTime
            mti = tma.attrib.get('msgTime')
            #nam TRACON
            nam = self.check_none(trn.findtext('urn:nam', None, ns))
            outStr = '{},{},{}'.format(mti,artcc,nam)
            self.adp_trn_file.write(outStr + '\n')

    def parse_gans(self,root,tma,adp,gans,ns,line):
                artcc = root.attrib.get('envSrce')
                artcc = artcc.split('.')[1]
#        if (artcc == self.artcc):
                tra = self.check_none(adp.findtext('urn:tra', None, ns))
#            if ((tra == self.tracon) | (tra == 'EDC')):
                for gan in gans.findall('urn:gan', ns):
                    #mti msgTime
                    mti = tma.attrib.get('msgTime')
                    #nam Gate
                    nam = self.check_none(gan.findtext('urn:nam', None, ns))
                    outStr = ''.format(mti,artcc,tra,nam)
                    self.adp_gans_file.write(outStr + '\n')

    def parse_mrns(self,root,tma,adp,mrns,ns,line):
                artcc = root.attrib.get('envSrce')
                artcc = artcc.split('.')[1]
#        if (artcc == self.artcc):
                tra = self.check_none(adp.findtext('urn:tra', None, ns))
#            if ((tra == self.tracon) | (tra == 'EDC')):
                for mrn in mrns.findall('urn:mrn', ns):
                    #mti msgTime
                    mti = tma.attrib.get('msgTime')
                    #nam Meter Reference Point
                    nam = self.check_none(mrn.findtext('urn:nam', None, ns))
                    #mrt MRP Type
                    mrt = self.check_none(mrn.findtext('urn:mrt', None, ns))
                    #gat Gate
                    gat = self.check_none(mrn.findtext('urn:gat', None, ns))
                    #mfx Meter Fix
                    mfx = self.check_none(mrn.findtext('urn:mfx', None, ns))
                    #oma
                    oma = self.check_none(mrn.findtext('urn:oma', None, ns))
                    #ooa
                    ooa = self.check_none(mrn.findtext('urn:ooa', None, ns))
                    #o3a
                    o3a = self.check_none(mrn.findtext('urn:o3a', None, ns))
                    #o4a
                    o4a = self.check_none(mrn.findtext('urn:o4a', None, ns))
                    #lat Latitude
                    lat = self.check_none(mrn.findtext('urn:lat', None, ns))
                    #lon Longitude
                    lon = self.check_none(mrn.findtext('urn:lon', None, ns))
                    #rad
                    rad = self.check_none(mrn.findtext('urn:rad', None, ns))
                    #lan
                    lan = self.check_none(mrn.findtext('urn:lan', None, ns))
                    #ran
                    ran = self.check_none(mrn.findtext('urn:ran', None, ns))
                    #ahi
                    ahi = self.check_none(mrn.findtext('urn:ahi', None, ns))
                    #alo
                    alo = self.check_none(mrn.findtext('urn:alo', None, ns))
                    outStr = '{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}'.format(mti,artcc,tra,nam,mrt,gat,mfx,oma,ooa,o3a,o4a,lat,lon,rad,lan,ran,ahi,alo)
                    self.adp_mrns_file.write(outStr + '\n')

    def parse_scns(self,root,tma,adp,scns,ns,line):
                artcc = root.attrib.get('envSrce')
                artcc = artcc.split('.')[1]
#        if (artcc == self.artcc):
                tra = self.check_none(adp.findtext('urn:tra', None, ns))
            #print(line + '\n')
#            if ((tra == self.tracon) | (tra == 'EDC')):
                #mti msgTime
                mti = tma.attrib.get('msgTime')
                for scn in scns.findall('urn:scn', ns):
                    nam = self.check_none(scn.findtext('urn:nam', None, ns))
                    outStr = '{},{},{},{}'.format(mti,artcc,tra,nam)
                    self.adp_scns_file.write(outStr + '\n')

    def parse_apns(self,root,tma,adp,apns,ns,line):
                artcc = root.attrib.get('envSrce')
                artcc = artcc.split('.')[1]
#        if (artcc == self.artcc):
                tra = self.check_none(adp.findtext('urn:tra', None, ns))
            #print(line + '\n')
#            if ((tra == self.tracon) | (tra == 'EDC')):
                for apn in apns.findall('urn:apn', ns):
                    #mti msgTime
                    mti = tma.attrib.get('msgTime')
                    #anam 
                    anam = self.check_none(apn.findtext('urn:nam', None, ns))
                    #rwns
                    rwyStr = ''
                    for rwns in apn.findall('urn:rwns', ns):
                        for rwn in rwns.findall('urn:rwn', ns):
                            #rnam
                            rnam = self.check_none(rwn.findtext('urn:nam', None, ns))
                            rwyStr += rnam + ' '
                    outStr = '{},{},{},{},{}'.format(mti,artcc,tra,anam,rwyStr.strip())
                    self.adp_apns_rwy_file.write(outStr + '\n')
                    #cfns
                    for cfns in apn.findall('urn:cfns', ns):
                        for cfn in cfns.findall('urn:cfn', ns):
                            #cnam
                            cnam = self.check_none(cfn.findtext('urn:nam', None, ns))
                            #rwys
                            rwyStr = ''
                            for rwys in cfn.findall('urn:rwys', ns):
                                for rwy in rwys.findall('urn:rwy', ns):
                                    #rnam
                                    rnam = self.check_none(rwy.text)
                                    rwyStr += str(rnam) + ' '
                                outStr = '{},{},{},{},{},{}'.format(mti,artcc,tra,anam,cnam,rwyStr.strip())
                                self.adp_apns_cfg_file.write(outStr + '\n')
                            if (len(rwyStr) == 0):
                                outStr = '{},{},{},{},{},{}'.format(mti,artcc,tra,anam,cnam,rwyStr.strip())
                                self.adp_apns_cfg_file.write(outStr + '\n')
                                
    def check_none(self,s):
        if (type(s) == type(None)):
            return('')
        elif (s == 'None'):
            return('')
        else:
            return(s)

    ### Main process flow
    def process_swim_data(self):
        self.run_message_processing()
    
if __name__ == '__main__':
    tbsw = TbfmSwimParser()
    tbsw.inFile = 'tbfm-swim_20200130_2200.xml.bz2'
    tbsw.process_command_line()
    tbsw.process_swim_data()
