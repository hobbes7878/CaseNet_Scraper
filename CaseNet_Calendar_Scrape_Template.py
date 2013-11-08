import mechanize
import cookielib
import urllib2
import csv
import re
from BeautifulSoup import BeautifulSoup
import time
from datetime import date, timedelta
import math
import random
import sys
import psycopg2

######### SETTING UP ##########

###Database Connect###
conn=psycopg2.connect("dbname=CaseNet user=postgres password=")
cur=conn.cursor()
SQL="INSERT INTO casenums_2011_13_Compare (casenum, casedate, casecourt, querydate) VALUES (%s, %s, %s, %s);"

###Timestamp###
stamp_start=time.clock()
today = date.today()

###Open output file###
# handle = open('Cases_Calendar.csv', 'a')
# outfile = csv.writer(handle)

###Open Error output file###
handle = open('Processing_errors_2011.csv', 'a')
error_outfile = csv.writer(handle)

###Browser###
br = mechanize.Browser()

###Cookie Jar###
cj = cookielib.LWPCookieJar()
br.set_cookiejar(cj)

###Browser options###
br.set_handle_equiv(True)
br.set_handle_gzip(True)
br.set_handle_redirect(True)
br.set_handle_referer(True)
br.set_handle_robots(False)

##Follows refresh 0 but not hangs on refresh > 0##
br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

###Mechanize Debugging###
#~ br.set_debug_http(True)
#~ br.set_debug_redirects(True)
#~ br.set_debug_responses(True)

###User-Agent###
br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]

######### COURTS LIST & DICTIONARIES #########
courts=['SMPDB0001_CT07','SMPDB0004_CT13','SMPDB0002_CT23','SMPDB0005_CT31']
#Courts already complete: 'OSCDB0024_SUP','SMPDB0005_EAP','SMPDB0001_SAP','SMPDB0001_WAP','SMPDB0002_CT01','SMPDB0004_CT02', 'SMPDB0002_CT03','SMPDB0003_CT04','SMPDB0001_CT05','SMPDB0003_CT06','SMPDB0001_CT07', 'SMPDB0004_CT08','SMPDB0001_CT09','SMPDB0004_CT10','SMPDB0004_CT11', 'SMPDB0002_CT12','SMPDB0004_CT13','SMPDB0001_CT14','SMPDB0003_CT15','SMPDB0017_CT16','SMPDB0003_CT17','SMPDB0002_CT18','OSCDB0024_CT19','SMPDB0002_CT20','SMPDB0009_CT21','SMPDB0010_CT22','SMPDB0002_CT23','SMPDB0005_CT24','SMPDB0002_CT25','SMPDB0004_CT26','SMPDB0005_CT27','SMPDB0004_CT28','SMPDB0001_CT29','SMPDB0003_CT30','SMPDB0005_CT31','SMPDB0001_CT32','SMPDB0003_CT33','SMPDB0005_CT34','SMPDB0001_CT35','SMPDB0004_CT36','SMPDB0005_CT37','SMPDB0003_CT38','SMPDB0003_CT39','SMPDB0001_CT40','SMPDB0003_CT41','SMPDB0005_CT42','SMPDB0005_CT43','SMPDB0002_CT44','SMPDB0005_CT45'
#Took out fine colletion center: 'OSCDB0013_FCC',

courtsdict={'OSCDB0024_SUP':'Supreme Court','SMPDB0005_EAP':'Eastern Appellate','SMPDB0001_SAP':'Southern Appellate','SMPDB0001_WAP':'Western Appellate','OSCDB0013_FCC':'Fine Collection Center','SMPDB0002_CT01':'1st Judicial Circuit (Clark, Schuyler & Scotland Counties)','SMPDB0004_CT02':'2nd Judicial Circuit (Adair, Knox & Lewis Counties)', 'SMPDB0002_CT03':'3rd Judicial Circuit (Grundy, Harrison, Mercer & Putnam Counties)','SMPDB0003_CT04':'4th Judicial Circuit (Atchison, Gentry, Holt, Nodaway & Worth Counties)','SMPDB0001_CT05':'5th Judicial Circuit (Andrew & Buchanan Counties)','SMPDB0003_CT06':'6th Judicial Circuit (Platte County)','SMPDB0001_CT07':'7th Judicial Circuit (Clay County)','SMPDB0004_CT08':'8th Judicial Circuit (Carroll & Ray Counties)','SMPDB0001_CT09':'9th Judicial Circuit (Sullivan, Linn & Chariton Counties)','SMPDB0004_CT10':'10th Judicial Circuit (Marion, Monroe & Ralls Counties)','SMPDB0004_CT11':'11th Judicial Circuit (St. Charles County)', 'SMPDB0002_CT12':'12th Judicial Circuit (Audrain, Montgomery & Warren Counties)','SMPDB0004_CT13':'13th Judicial Circuit (Boone & Callaway Counties)','SMPDB0001_CT14':'14th Judicial Circuit (Randolph & Howard Counties)','SMPDB0003_CT15':'15th Judicial Circuit (Saline & Lafayette Counties)','SMPDB0017_CT16':'16th Judicial Circuit (Jackson County)','SMPDB0003_CT17':'17th Judicial Circuit (Cass & Johnson Counties)','SMPDB0002_CT18':'18th Judicial Circuit (Cooper & Pettis Counties)','OSCDB0024_CT19':'19th Judicial Circuit (Cole County)','SMPDB0002_CT20':'20th Judicial Circuit (Franklin, Gasconade & Osage Counties)','SMPDB0009_CT21':'21st Judicial Circuit (St. Louis County)','SMPDB0010_CT22':'22nd Judicial Circuit (City of St. Louis)','SMPDB0002_CT23':'23rd Judicial Circuit (Jefferson County)','SMPDB0005_CT24':'24th Judicial Circuit (Madison, St. Genevieve, St.  Francois & Washington Counties)','SMPDB0002_CT25':'25th Judicial Circuit (Maries, Phelps, Pulaski & Texas Counties)','SMPDB0004_CT26':'26th Judicial Circuit (Camden, Laclede, Miller, Morgan & Moniteau Counties)','SMPDB0005_CT27':'27th Judicial Circuit (Bates, Henry & St. Clair Counties)','SMPDB0004_CT28':'28th Judicial Circuit (Barton, Cedar, Dade & Vernon Counties)','SMPDB0001_CT29':'29th Judicial Circuit (Jasper County)','SMPDB0003_CT30':'30th Judicial Circuit (Benton, Dallas, Hickory, Polk & Webster Counties)','SMPDB0005_CT31':'31st Judicial Circuit (Greene County)','SMPDB0001_CT32':'32nd Judicial Circuit ( Bollinger, Cape Girardeau & Perry Counties)','SMPDB0003_CT33':'33rd Judicial Circuit (Mississippi & Scott Counties)','SMPDB0005_CT34':'34th Judicial Circuit (New Madrid & Pemiscot Counties)','SMPDB0001_CT35':'35th Judicial Circuit (Dunklin & Stoddard Counties)','SMPDB0004_CT36':'36th Judicial Circuit (Butler & Ripley Counties)','SMPDB0005_CT37':'37th Judicial Circuit (Carter, Howell, Oregon & Shannon Counties)','SMPDB0003_CT38':'38th Judicial Circuit (Taney & Christian Counties)','SMPDB0003_CT39':'39th Judicial Circuit (Barry, Lawrence & Stone Counties)','SMPDB0001_CT40':'40th Judicial Circuit (Newton & McDonald Counties)','SMPDB0003_CT41':'41st Judicial Circuit (Macon & Shelby Counties)','SMPDB0005_CT42':'42nd Judicial Circuit (Dent, Crawford, Iron, Reynolds & Wayne Counties)','SMPDB0005_CT43':'43rd Judicial Circuit (DeKalb, Daviess, Clinton, Caldwell & Livingston Counties)','SMPDB0002_CT44':'44th Judicial Circuit (Douglas, Ozark & Wright Counties)','SMPDB0005_CT45':'45th Judicial Circuit (Lincoln & Pike Counties)'}
courtsdict_reverse= dict([(v,k) for (k,v) in courtsdict.iteritems()])

# ### Under construction... ### Check for courts already scraped using list comprehension.. somehow..
# cur.execute("SELECT casecourt FROM casenums_2011_13_Compare GROUP BY casecourt;")
# cases_scraped = cur.fetchall()
# #cases_scraped=[ cs for cs in cases_scraped if cs[0] not in courtsdict_reverse]



######### DATE LIST #########

##CaseNet allows searches in 7-day increments.

#Number of years to Search
print "Welcome to your scrape!"
YEARS=input("How many years' records would you like to scrape today? ")

date_range=[]
date_mark = date(2012,1,2) #end date or date.today()
date_stop=date_mark-timedelta(days=(365*YEARS))

while date_mark > date_stop:
	date_mark=date_mark-timedelta(days=7)
	day= date_mark.strftime('%m/%d/%Y')
	date_range.append(day)



########### QUERY DOCKET ###########

print "ALL RISE. SCRAPE IS IN SESSION."

courtlen=len(courts)
datelen=len(date_range)

ci=1 #counter

all_cases=[]

for c in courts:

	casecount=0
	stamp_stage=time.clock()
	print "Processing Court: No. ",ci," of ",courtlen," |",courtsdict[c]

	for d in date_range:
		except_count=1 #error counter for try loop
		while True: #exception loop
			try:

				sys.stdout.write("*")

				cases=[]
				dates=[]

				###Open Site###
				br.open('https://www.courts.mo.gov/casenet/cases/filingDateSearch.do')

				#Select Form
				br.form = list(br.forms())[2]
				
				#set controls to writeable
				br.form.set_all_readonly(False)

				#Set Court to 13th District
				br["inputVO.courtId"] = c
				br["inputVO.selectedIndexCourt"] = "0"
				br["inputVO.selectedIndexCourt"] = c
				br["inputVO.startDate"] = d

				#Submit Search
				response=br.submit("findButton")

				#read response
				response_read=response.read()

				#Get cases on first page
				cases=re.findall(r"goToThisCase\(\'(.+?)\'", response_read)
				all_cases=re.findall(r"goToThisCase\(\'(.+?)\'", response_read)
				
				#Get case dates on first page (regex excludes other page dates)
				datesreg=[]
				datesreg=re.findall(r"<td class=\"td\d+?\">\d{2}\/\d{2}\/\d{4}", response_read)
				for dr in datesreg:
					dates.append(dr[16:])
				

				##Get length to iterate across pages, ie, num of cases.##
				Iterate=len(cases)

				###Set Form###   
				br.form = list(br.forms())[0]

				###Get case and page numbers###
				T_Cases = int(br["inputVO.totalRecords"])
				casecount+=T_Cases
				Pages = math.ceil(T_Cases /float(Iterate))
				Page_Range=range(16, T_Cases,Iterate)

				page_count=1

				###Iterate over pages###
				for p in Page_Range:
					page_count+=1
					br.form = list(br.forms())[0]
					br.form.set_all_readonly(False)
					br["inputVO.startingRecord"]=str(p)
					response=br.submit()
					response_read=response.read()
					#Case Pull
					Found=re.findall(r"goToThisCase\(\'(.+?)\'", response_read)
					f_count=1
					for f in Found:
						f_count+=1
						cases.append(f)
						all_cases.append(f)
					#Date Pull
					Datefound=re.findall(r"<td class=\"td\d+?\">\d{2}\/\d{2}\/\d{4}", response_read)
					df_count=1
					for df in Datefound:
						df_count+=1
						dates.append(df[16:])

					time.sleep(random.randint(1,3))
					br.back()
				# outfile.writerow(cases)

				#Create Dict of cases and dates
				casedict=dict(zip(cases,dates))

				#SEND DICT TO DATABASE
				for cd in casedict:
					casedata = (cd,)
					datecase = (casedict[cd],)
					casecourt = (courtsdict[c],)
					dateweek = (d,)
					cur.execute(SQL, (casedata, datecase, casecourt, dateweek))
					conn.commit()


			except Exception, e:
				except_count+=1
				sys.stdout.write("#")
				error_outfile.writerow([except_count,courtsdict[c],d,page_count,f_count,df_count,str(e)])
				continue #retry on exception until processed
			break

	ci+=1 #counter
	stamp_end=time.clock()

	time.sleep(random.randint(1,5))
	print " ",(int(stamp_end-stamp_stage)/60),":",(int(stamp_end-stamp_stage)%60)," | ",casecount," cases"

###SALUTATIONS###
stamp_end=time.clock()
print "SCRAPE COMPLETE:"
print "Processed in ",(int(stamp_end-stamp_start)/60)," min. ",(int(stamp_end-stamp_start)%60)," sec."
print "That's mm-mmm good!"

###CLOSE DATABASE CONNECTION###
cur.close()
conn.close()
