# detailedtbfmswim
The python scripts described here provide example source code that can be used for deeper dive analysis of public TBFM SWIM aviation data.

The expectation is that analysts and developers will be starting with an archive of TBFM SWIM data stored in Zulu time folder structure.
While raw storage is by Zulu day, the reports and views of the data desired are in local days.
![](images/local_day.JPG)


The first step in flattening to CSV takes input files from two folders as well as a target date. The python command to perform this is listed below. In this example, we tell the script that we are interested in April 4th, 2019 and it needs to pull information from two separate zulu day archives to create one local day output. We are also asking the script to provide its output in the 'flattened_dir' directory.

> python TbfmSwimFromRawArchive.py –m archive –z 20190401 –r 20190401,20190402 –o flattened_dir 


The results of step 1 flattening are 22 distinct CSV files
This separates all the unique messages into their own CSV file
This is often helpful for analysis given each TBFM message has a different structure
It is rare to use more than 6-8 different message types, but all are stored in case they are needed
The output is not in compressed form, but for longer term storage it would benefit from compression. The uncompressed output from 04/01/2019 is 2.9GB. However, after bzip2 compression it is 339MB on disk (8.6 to 1 compression ratio)

![](images/flattened_csv_list.JPG)


Step 2 - Estimating TBFM Surface Delay 

Beginning with the output from step 1 processing, each SWIM AIR ‘flt’ message is parsed
The resulting output from step 2 is only those flights that are *potentially* an APREQ from TBFM
A later step will winnow this list down into a set of flights with varying levels of confidence
This step relies heavily upon the ‘std’ and ‘ctm’ messages from ‘flt’ data given SWIM has no ‘ready time’
Unfortunately, some of these messages appear to be missing from the TBFM SWIM dataset. More analysis is required to understand why these key messages are missing
The following command will pull relevant flattened CSV file data from 20190401, generate a list of potential APREQ data and output the results into a single file in the air_flt_w_delay_est directory
You will need to create the output directory 

> python TbfmSwimScheduledDataFromFly.py flattened_dir 20190401 –o air_flt_w_delay_est 


The output is approximately 2MB

Step 3 - Creating a Dataset for Analysis

Leveraging the output from both step1 and step 2, the next step is to create a more complete dataset for analysis
This pulls in data from several other flattened CSV files for this day and merges it with the output of step 2
In the process of doing so, this requires both intra-center and inter-center duplicate flight processing as described on the next slide
The following command will pull relevant data from 20190401, find the best ETAs/STAs and other relevant SWIM data to match the APREQ flight, perform quality checks on the estimated APREQ, and output the results to a single file per day
You will need to create the output directory 
The output is 73 columns of data and uses 2.7MB of disk space

> python TbfmMergedSummary.py flattened_dir  air_flt_w_delay_est 20190401 –o merged_summary 

