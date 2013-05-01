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
from pprint import pprint


######### SETTING UP ##########

###Timestamp###
stamp1=time.clock()

def taketime():
	stage_stamp=time.clock()
	stamp=str(int(stage_stamp-start_stamp)/60)+":"+str(int(stage_stamp-start_stamp)%60)
	sys.stdout.write(stamp)


###Database Connect###
conn=psycopg2.connect("dbname=CaseNet user=postgres password=tomcat")
cur=conn.cursor()

###Base Handel###
base_url = 'https://www.courts.mo.gov/casenet/cases/'

###Open output file###
# handle = open('Cases_Detail.csv', 'a')
# outfile = csv.writer(handle)

###Open Error output files###
handle = open('DetailScrape_Errors.csv', 'a')
error_outfile = csv.writer(handle)

handler = open('Detail_NotScraped.csv', 'a')
notscrape_outfile = csv.writer(handler)

####### MECHANIZE BROWSER #######
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

##### Court Dictionaries #####
courtsdict={'OSCDB0024_SUP':'Supreme Court','SMPDB0005_EAP':'Eastern Appellate','SMPDB0001_SAP':'Southern Appellate','SMPDB0001_WAP':'Western Appellate','OSCDB0013_FCC':'Fine Collection Center','SMPDB0002_CT01':'1st Judicial Circuit (Clark, Schuyler & Scotland Counties)','SMPDB0004_CT02':'2nd Judicial Circuit (Adair, Knox & Lewis Counties)', 'SMPDB0002_CT03':'3rd Judicial Circuit (Grundy, Harrison, Mercer & Putnam Counties)','SMPDB0003_CT04':'4th Judicial Circuit (Atchison, Gentry, Holt, Nodaway & Worth Counties)','SMPDB0001_CT05':'5th Judicial Circuit (Andrew & Buchanan Counties)','SMPDB0003_CT06':'6th Judicial Circuit (Platte County)','SMPDB0001_CT07':'7th Judicial Circuit (Clay County)','SMPDB0004_CT08':'8th Judicial Circuit (Carroll & Ray Counties)','SMPDB0001_CT09':'9th Judicial Circuit (Sullivan, Linn & Chariton Counties)','SMPDB0004_CT10':'10th Judicial Circuit (Marion, Monroe & Ralls Counties)','SMPDB0004_CT11':'11th Judicial Circuit (St. Charles County)', 'SMPDB0002_CT12':'12th Judicial Circuit (Audrain, Montgomery & Warren Counties)','SMPDB0004_CT13':'13th Judicial Circuit (Boone & Callaway Counties)','SMPDB0001_CT14':'14th Judicial Circuit (Randolph & Howard Counties)','SMPDB0003_CT15':'15th Judicial Circuit (Saline & Lafayette Counties)','SMPDB0017_CT16':'16th Judicial Circuit (Jackson County)','SMPDB0003_CT17':'17th Judicial Circuit (Cass & Johnson Counties)','SMPDB0002_CT18':'18th Judicial Circuit (Cooper & Pettis Counties)','OSCDB0024_CT19':'19th Judicial Circuit (Cole County)','SMPDB0002_CT20':'20th Judicial Circuit (Franklin, Gasconade & Osage Counties)','SMPDB0009_CT21':'21st Judicial Circuit (St. Louis County)','SMPDB0010_CT22':'22nd Judicial Circuit (City of St. Louis)','SMPDB0002_CT23':'23rd Judicial Circuit (Jefferson County)','SMPDB0005_CT24':'24th Judicial Circuit (Madison, St. Genevieve, St.  Francois & Washington Counties)','SMPDB0002_CT25':'25th Judicial Circuit (Maries, Phelps, Pulaski & Texas Counties)','SMPDB0004_CT26':'26th Judicial Circuit (Camden, Laclede, Miller, Morgan & Moniteau Counties)','SMPDB0005_CT27':'27th Judicial Circuit (Bates, Henry & St. Clair Counties)','SMPDB0004_CT28':'28th Judicial Circuit (Barton, Cedar, Dade & Vernon Counties)','SMPDB0001_CT29':'29th Judicial Circuit (Jasper County)','SMPDB0003_CT30':'30th Judicial Circuit (Benton, Dallas, Hickory, Polk & Webster Counties)','SMPDB0005_CT31':'31st Judicial Circuit (Greene County)','SMPDB0001_CT32':'32nd Judicial Circuit ( Bollinger, Cape Girardeau & Perry Counties)','SMPDB0003_CT33':'33rd Judicial Circuit (Mississippi & Scott Counties)','SMPDB0005_CT34':'34th Judicial Circuit (New Madrid & Pemiscot Counties)','SMPDB0001_CT35':'35th Judicial Circuit (Dunklin & Stoddard Counties)','SMPDB0004_CT36':'36th Judicial Circuit (Butler & Ripley Counties)','SMPDB0005_CT37':'37th Judicial Circuit (Carter, Howell, Oregon & Shannon Counties)','SMPDB0003_CT38':'38th Judicial Circuit (Taney & Christian Counties)','SMPDB0003_CT39':'39th Judicial Circuit (Barry, Lawrence & Stone Counties)','SMPDB0001_CT40':'40th Judicial Circuit (Newton & McDonald Counties)','SMPDB0003_CT41':'41st Judicial Circuit (Macon & Shelby Counties)','SMPDB0005_CT42':'42nd Judicial Circuit (Dent, Crawford, Iron, Reynolds & Wayne Counties)','SMPDB0005_CT43':'43rd Judicial Circuit (DeKalb, Daviess, Clinton, Caldwell & Livingston Counties)','SMPDB0002_CT44':'44th Judicial Circuit (Douglas, Ozark & Wright Counties)','SMPDB0005_CT45':'45th Judicial Circuit (Lincoln & Pike Counties)'}
courtsdict_reverse= dict([(v,k) for (k,v) in courtsdict.iteritems()])



###QUERY CASENUM DATABASE###
cur.execute("SELECT casenum, casecourt FROM casenums_2012 WHERE casecourt='13th Judicial Circuit (Boone & Callaway Counties)';")
case_query=cur.fetchall() #returns list of tuples
print "case_query: ", len(case_query)

cur.execute("SELECT case_index FROM casedata;")
cases_scraped=cur.fetchall()
print "cases_scraped: ", len(cases_scraped)

cur.execute("SELECT casenum FROM casedata_errors;")
case_errors=cur.fetchall()
print "case_errors: ", len(case_errors)

### Comprehensions to check for cases already scraped ###
case_query = [cq for cq in case_query if cq[:1] not in cases_scraped] #dups in scraped db
print "cq - scraped: ", len(case_query)
case_query = [cq for cq in case_query if cq[:1] not in case_errors] #dups in errors db
print "cq - errors: ", len(case_query)




lencase=len(case_query)
casei=0


######### CASE DETAIL SCRAPE #########
for cq in reversed(case_query):  #reversed()    #For multiple scrapers
	sys.stdout.write(str(cq[:1]))

	### Redundant Redundant Check ###    #For multiple scrapers
	cur.execute("SELECT case_index FROM casedata;")
	cases_scraped=cur.fetchall()

	if cq[:1] in cases_scraped:
		print "DUP"
	else:

		# ### Time Check ###   Only run scraper outside working hours, 8-5? Uncomment.
		# punchclock =int(time.strftime("%H"))
		# if punchclock >= 8 and punchclock <= 17:
		# 	sleepsecs=(17-punchclock)*(60**2)
		# 	print "Sleepy... ZZZzzzzzz   ",(17-punchclock)," hrs."
		# 	time.sleep(sleepsecs)
			

		casei+=1
		case_retry=1

		while True: #exception loop
			try:
				start_stamp=time.clock()

				###### Search Case ######
				br.open('https://www.courts.mo.gov/casenet/cases/searchCases.do?searchType=caseNumber')

				Case_ID = cq[0]
				Court_ID = courtsdict_reverse[cq[1]]

				###Select Form###
				br.form = list(br.forms())[2]
				br.form.set_all_readonly(False)

				br["inputVO.courtId"]= Court_ID
				br["inputVO.caseNo"] = Case_ID

				###Submit Search -- >Case Header###
				response=br.submit("findButton")
				soup = BeautifulSoup(response)

				table=soup.find('table', cellspacing="2" )


				###### HEADER SCRAPE ######

				case_data=[Case_ID]
				label_keys=['case_index']

				for tr in table.findAll('tr'):	
					###Get label cells### (Dict Keys)
					tds=tr.findAll("td", {'class' : 'header'})
					for td in tds:
						text=(td.text).encode('utf-8')
						text= "casehead" + text.replace('&nbsp;','_').replace(' ','_').lower()
						label_keys.append(text)
					
					###Get data cells### (Dict Values)
					tds=tr.findAll("td", {'class' : 'td1'})
					for td in tds:
						text=(td.text).encode('utf-8')
						text=text.strip('&nbsp;')
						case_data.append(text)

				#Write dict
				datadict=dict(zip(label_keys, case_data))
				#pprint(datadict)

				###Call controls in form###   
				br.form = list(br.forms())[3]

				br.form.set_all_readonly(False)

				br["inputVO.caseNumber"]= Case_ID
				br["inputVO.courtId"]= Court_ID

				response=br.submit()
				response_read=response.read()
				soup = BeautifulSoup(response)
				
				### Pages + BaseURL ###
				page_list_reg=re.findall(r"submitForCaseDetails.+?do", response_read)
				page_list=[]
				for pl in page_list_reg:
					page_list.append(pl[22:])

				### REMOVE ANY DATA TABS NOT TO BE SCRAPED ###
				unwanteds=["filings.do","events.do","garnishment.do"] #"searchDockets.do","service.do",
				for uw in unwanteds:
					try:
						page_list.remove(uw)
					except:
						continue

				

				###Select and Copy form values###
				br.form = list(br.forms())[2]
				control_names=[]
				control_values=[]
				for c in br.form.controls:
					control_names.append(c.name)
					control_values.append(c.value)


				###Iterate through tabs###
				for p in page_list:
					sys.stdout.write((p[:1]))
					sys.stdout.write(" ")
					taketime()
					sys.stdout.write("|")

					case_data=[]
					label_keys=[]
					seperator_data=[]
					seperator_keys=[]

					response=br.open(base_url+p)
					
					###Select form###
					br.select_form(name="casePalletteForm")
					br.form.set_all_readonly(False)

					###Write all values to form###
					for v, n in zip(control_values, control_names):
						br[n]=v

					response=br.submit()
					soup=BeautifulSoup(response)
					
					###Get data cells### (Dict Values)
					tds=soup.findAll('td', {'class': 'detailData'})
					for td in tds:
						text=(td.text).encode('utf-8')
						text = text.replace('&nbsp;',' ').replace('\t','').replace('\r','').replace('\n','').strip() + " " #Add space for non blank idnetifier
						case_data.append(text)
					
					###Get header cells### (Dict Keys)
					tds=soup.findAll('td', {'class': 'detailLabels'})
					for td in tds:
						text=(td.text).encode('utf-8')
						text = p[:-3].lower() + "_" + text.replace('&nbsp;',' ').replace('\t','').replace('\r','').replace('\n','').strip(":").strip().replace(' ','_').lower()
						text=re.sub('[^0-9a-zA-Z_]+','_',text)

						i=1 #No duplicate dict keys
						texti = text
						while texti in label_keys:
							texti = text + str(i)
							i+=1
						label_keys.append(texti)

					###Add Label Dict Keys### (Don't lose data without associated label)
					i=1
					while len(label_keys)<len(case_data):
						Label_Key=p[:-3].lower() + "_data"+str(i)
						label_keys.append(Label_Key)
						i+=1

					###Get seperator(sic) cells### (Another data cell type) (Dict Values)
					tds=soup.findAll('td', {'class': 'detailSeperator'})
					for td in tds:
						text=(td.text).encode('utf-8')
						text = text.replace('&nbsp;',' ').replace('\t','').replace('\r','').replace('\n','').strip() + " " 
						seperator_data.append(text)

					###Add Seperator Dict Keys###
					i=1
					while len(seperator_keys)<len(seperator_data):
						Sep_Key=p[:-3].lower() + "_sep"+str(i)
						seperator_keys.append(Sep_Key)
						i+=1


					###Compile Dicts###
					datadict=dict(datadict.items()+(dict(zip(label_keys, case_data))).items())
					datadict=dict(datadict.items()+(dict(zip(seperator_keys, seperator_data))).items())
					
					#time.sleep(random.randint(1,3))

				#Get column names from database
				cur.execute("SELECT * FROM casedata;")
				col_names = [desc[0] for desc in cur.description]
				
				#Get column names from dict
				dict_keys=datadict.keys()

				### Create any new columns in database ###
				for dk in dict_keys:
					if dk in col_names:
						pass
					else:
						SQL="ALTER TABLE casedata ADD " + str(dk) +" varchar;"
						cur.execute(SQL)
						conn.commit()



				### Write dict to database ###
				SQL="INSERT INTO casedata (" + str(datadict.keys()).strip("[").strip("]").replace("'","") + ") VALUES (" +str(datadict.values()).strip("[").strip("]") + ");" 
				cur.execute(SQL)
				conn.commit()

				#pprint(datadict)
						
				
				#Time Keeping and Process Status
				stage_stamp=time.clock()
				print (int(stage_stamp-start_stamp)/60),":",(int(stage_stamp-start_stamp)%60)," #",casei,"/",lencase
				#time.sleep(random.randint(1,3))

			except Exception, e:

				if case_retry <=5:
					sys.stdout.write("#")
					print "Err:",e
					
					error_outfile.writerow([cq,str(e)])
					case_retry+=1
					continue #retry on exception until processed
				else:
					print ""
					print "SCRAPE SKIPPED FOR: ", Case_ID
					notscrape_outfile.writerow([cq, Court_ID, str(e)])
					
					### Write error to database ###
					try:
						SQL="INSERT INTO casedata_errors (casenum, court, error) VALUES (%s, %s, %s);"
						cqt=(Case_ID,)
						ct=((courtsdict[Court_ID]),)
						et=(str(e),)
						cur.execute(SQL, (cqt, ct, et))
						conn.commit()
					except:
						print "ERROR NOT WRITTEN TO DATABASE!", 
						print "Error: ",e

					break
					

			break


###SALUTATIONS###
stamp2=time.clock()
print "SCRAPE COMPLETE:"
print "Processed in ",(int(stamp2-stamp1)/60)," min. ",(int(stamp2-stamp1)%60)," sec."

###CLOSE DATABASE CONNECTION###
cur.close()
conn.close()