from google.oauth2.service_account import Credentials
import csv
import json
from googleapiclient import discovery
import pandas as pd
service_account_info = json.load(open('./auth.json'), strict=False)
creds = Credentials.from_service_account_info(service_account_info)

service = discovery.build('sheets', 'v4', credentials=creds)
spreadsheet_id = '1JE4dFUycVV1KevVaJhej92JU95fANGyFG0aBJde9_e0'
range_ = 'School Overview Data'
# df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]})

# # df = df.values.tolist()



# #
#  #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# Created on Mon Nov 21 14:18:27 2022

# @author: swapnilsangal
# """


from python_graphql_client import GraphqlClient
import pandas as pd

Graphql_client = GraphqlClient(endpoint="https://api.tekie.in/graphql/core",headers={'Authorization':'ZXlKaGJHY2lPaUpJVXpJMU5pSXNJblI1Y0NJNklrcFhWQ0o5LmV5SmhjSEJKYm1adklqcDdJbTVoYldVaU9pSjBaV3RwWlZSdGN5SXNJblI1Y0dVaU9pSnpkR0YwYVdNaWZTd2lhV0YwSWpveE5UZzVOVE13T0RVNExDSmxlSEFpT2pFNU1EVXhNRFk0TlRoOS43ck9ILW5fM2Z1dUR0TWFoTVdJUVhOQS1nejllX216Y0o5VlNOTHd1WmVvOjpleUpoYkdjaU9pSklVekkxTmlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKMWMyVnlTVzVtYnlJNmV5SnBaQ0k2SW1OcmVEZHdlbWc1YmpBd00zb3dkREZvWTNGaU1tUjZjbU1pTENKMWMyVnlibUZ0WlNJNklrOXRSSFZpWlhsNWVTSjlMQ0pwWVhRaU9qRTJOREkyTmprek9UTXNJbVY0Y0NJNk1UWTNOREl5TmprNU0zMC5WN2l5TTZXZE1SaTFRd3F3b2RGT2Vob2duc0xPVEUyNTNYdnMtLXV4d2NV'})

required_schools = ['Aurinko Academy', 'Gnan Shrishti School Of Excellence', 'BNR Memorial School', 'Prestige International School', 'Tunbridge High School', 'Christ Academy', 'St Joseph Academy', 'Chinmaya Vidyalaya Banashankari', 'Unicent School', 'Sri Vivekananda International School', 'Spring Dale School Mallapur', 'Olivemount Global School', 'KGS Matriculation Tirupur', 'NPS Gottigere', 'C.K.S. English School RK Puram', 'Harsha International Public School', 'SMS Vidyapeeth', 'C.K.S. English School BM Road', 'Janak Academy', 'Yenapoya', 'Carmel Teresa', 'Edify School', 'NPS Agara', 'KSR Akshara Academy', 'Balodyana English School', 'Glentree Academy, Whitefield', 'Vidya Dayini', 'Sahasraa high school', 'oxford global school Mangalore', 'United Public School', 'HH International', 'PACE', 'Dolphin Kanwar Nagar', 'Delhi Public School Ujjain', 'Wellwisher School', 'Vedant World School, Jaipur', 'Dolphins High School', 'Swamy Vivekananda School', 'St Thomas Residential School', 'Dolphin RP Center', 'Gangothri International Public School', 'Scholars School Rajpura', 'AVP Trust School', 'CHINMAYA VIDYALAYA BOISAR', 'Whitefield Global School', 'SLS International Gurukul', 'Dolphin Chaksu', 'PES SCHOOL', 'Glentree Academy, Sarjapur', 'Chinmaya International School Boisar', 'Carmel School Johrat', 'ARRS Academy']
Req_school = '''"Aurinko Academy", "Gnan Shrishti School Of Excellence", "BNR Memorial School", "Prestige International School", "Tunbridge High School", "Christ Academy", "St Joseph Academy", "Chinmaya Vidyalaya Banashankari", "Unicent School", "Sri Vivekananda International School", "Spring Dale School Mallapur", "Olivemount Global School", "KGS Matriculation Tirupur", "NPS Gottigere", "C.K.S. English School RK Puram", "Harsha International Public School", "SMS Vidyapeeth", "C.K.S. English School BM Road", "Janak Academy", "Yenapoya", "Carmel Teresa", "Edify School", "NPS Agara", "KSR Akshara Academy", "Balodyana English School", "Glentree Academy, Whitefield", "Vidya Dayini", "Sahasraa high school", "oxford global school Mangalore", "United Public School", "HH International", "PACE", "Dolphin Kanwar Nagar", "Delhi Public School Ujjain", "Wellwisher School", "Vedant World School, Jaipur", "Dolphins High School", "Swamy Vivekananda School", "St Thomas Residential School", "Dolphin RP Center", "Gangothri International Public School", "Scholars School Rajpura", "AVP Trust School", "CHINMAYA VIDYALAYA BOISAR", "Whitefield Global School", "SLS International Gurukul", "Dolphin Chaksu", "PES SCHOOL", "Glentree Academy, Sarjapur", "Chinmaya International School Boisar", "Carmel School Johrat", "ARRS Academy"'''

#%% sheet


#%% All batches data
#Fetching batches
Batches_query_structure_string = '''
    id
    classroomTitle
    code
    school{
      name
      studentsMeta{
        count
      }
      hubspotId
      teachers{
        user{
          name
        }
      }
    }
    coursePackage{
      title
      topics{
        topic{
          courses{
            title
          }
          title
          classType
        }
      }
    }
    coursePackageTopicRule {
     	topic{
        courses{
          title
        }
          title
          classType
        }
    }
   	classes{
      grade
      section
    }
    studentsMeta{
      count                                       
    }
    allottedMentor {
      name
    }
'''
All_batches_list = []
for I in range(0,4):
    Batches_detail_query = '''
    {
      first: batches(filter:{and:
                              [
      {createdAt_gte:"01/01/2022"},
      {type:b2b},
      {school_some: { name_in: ['''+Req_school+'''] } },
      {isTeacherTraining_not:true},
      {coursePackage_exists:true},
      {documentType:classroom}
    ]}, orderBy: createdAt_ASC, first: 250, skip:'''+str(250*I)+'''){
                                  %s
                                  }
                                  }
    ''' % (Batches_query_structure_string)
    
    Batches_data = Graphql_client.execute(query=Batches_detail_query)
    Batches_data = Batches_data['data']['first']
    All_batches_list.extend(Batches_data)
#%%
All_batches_list_processed = []
for Each_batch in All_batches_list:
    This_batch = [] 
    
    #1,2 School Name and Hubspot ID
    This_batch.append(Each_batch['school']['name'])
    This_batch.append(Each_batch['school']['hubspotId'])
    
    #3 School Student Count
    This_batch.append(Each_batch['school']['studentsMeta']['count'])
    
    #4 batch code
    This_batch.append(Each_batch['code'])
    
    #5 batch title
    This_batch_classroomTitle = Each_batch['classroomTitle']
    if This_batch_classroomTitle!=None:
        This_batch.append(This_batch_classroomTitle)
    else:
        This_batch.append(None)
    
    #6,7 Grade and Section
    This_batch_classes = Each_batch['classes']
    if len(This_batch_classes)==1:
        This_batch.extend([This_batch_classes[0]['grade'],This_batch_classes[0]['section']])
    else:
        print('Hello multiple classes found! What to do??')    
    
    #8 batch Teacher
    This_batch_mentor = Each_batch['allottedMentor']
    if This_batch_mentor!=None:
        This_batch.append(This_batch_mentor['name'])
    else:
        This_batch.append(None)
    
    #9 batch student count
    This_batch_student_count = Each_batch['studentsMeta']['count']
    This_batch.append(This_batch_student_count)
    
    #10 Batch course Package
    This_batch.append(Each_batch['coursePackage']['title'])
    
    #11,12 Lab Session count and courses
    This_batch_package_topicRule = Each_batch['coursePackageTopicRule']
    if len(This_batch_package_topicRule)>0:
        Lab_count=0
        Courses_set = set()
        for Each_session in This_batch_package_topicRule:
            if Each_session['topic']!=None and Each_session['topic']['classType']=='lab':
                Lab_count=Lab_count+1
                Courses_set.add(Each_session['topic']['courses'][0]['title'])
                
    else:
        Lab_count=0
        Courses_set = set()
        This_batch_course_package = Each_batch['coursePackage']['topics']
        for Each_session in This_batch_course_package:
            if Each_session['topic']!=None and Each_session['topic']['classType']=='lab':
                Lab_count=Lab_count+1
                Courses_set.add(Each_session['topic']['courses'][0]['title'])
    
    This_batch.append(Courses_set)
    This_batch.append(Lab_count)
    
    #13 School Teachers
    This_batch.append(Each_batch['school']['teachers'])
    
    #14 Classroom ID
    This_batch.append(Each_batch['id'])
    
    All_batches_list_processed.append(This_batch)

all_batches_df = pd.DataFrame(All_batches_list_processed,columns=['School','Hubspot ID','School Student Count','Batch Code','Classroom Title','Grade','Section','Teacher','Batch Student Count','Course Package Name','Batch Courses','No. of Assigned Lab Sessions','School Teachers','ClassroomId'])
all_batches_df = all_batches_df[all_batches_df['School'].isin(required_schools)]
all_batches_df['No. of courses'] = all_batches_df['Batch Courses'].apply(lambda x: len(x))
all_batches_df['Key_1'] = all_batches_df['School']+all_batches_df['ClassroomId']
all_batches_df['Key_2'] = all_batches_df['School']+all_batches_df['ClassroomId']+all_batches_df['Teacher']

#%% All school data
all_school_df = all_batches_df.groupby(['School'],as_index=False).agg({'School':'first','Hubspot ID':'first','School Student Count':'first','Grade':lambda x: sorted(list(set(x))),'Key_2':'count','Classroom Title':lambda x:list(x),'Batch Student Count':'sum','No. of Assigned Lab Sessions':'sum','School Teachers':'first','Teacher':lambda x:list(set(x))})
all_school_df['Hubspot Link'] = 'https://app.hubspot.com/contacts/7949159/deal/'+all_school_df['Hubspot ID']+'/'
all_school_df['No. of Teachers (TMS)'] = all_school_df['School Teachers'].apply(lambda x: len(x))
all_school_df = all_school_df.rename(columns=({'School Student Count':'Student Count (TMS)','Batch Student Count':'Student Count (Batches)','Grade':'Unique Grades','Key_2':'No. of Batches','Classroom Title':'Unique Batches','School Teachers':'Unique Teachers (TMS)','Teacher':'Unique Teachers (Batches)'}))

#%% All teacher data
def process_teachers(row):
    teacher_set=set()
    for each_teacher in row:
        teacher_set.add(each_teacher['user']['name'])
    return teacher_set

all_school_df['Unique Teachers (TMS)'] = all_school_df['Unique Teachers (TMS)'].apply(lambda x: process_teachers(x))
all_school_df['Unique Teachers (Batches)'] = all_school_df['Unique Teachers (Batches)'].apply(set)
def difference_teachers(a,b):
    diff = a.difference(b)
    return diff
all_school_df['Inactive Teachers'] = all_school_df.apply(lambda x: difference_teachers(x['Unique Teachers (TMS)'],x['Unique Teachers (Batches)']),axis=1)
all_school_df['Inactive Teachers Count'] = all_school_df['Inactive Teachers'].apply(len)
'''
P2
To be continued 
'''
#all_teacher_df = all_batches_df.groupby(['Key_2'],as_index=False).agg({'Teacher':'first','School':'first','Grade':'first','Section':'first','Classroom Title':'first','Batch Student Count':'sum','Course Package Name':'first','Key_2':'first'})
'''
duplicated_batches = all_batches_df[all_batches_df['Key_1'].duplicated(keep=False)]
batches_with_no_students = all_batches_df[all_batches_df['Batch Student Count']==0]
batches_with_no_teacher = all_batches_df[all_batches_df['Teacher'].isna()]
'''

print("graphquery.py completed")
#%%
from sqlalchemy import create_engine
import pandas as pd
AlchemyEngine = create_engine('postgresql+psycopg2://tekieadmin@reports-tekie:E4qpTYr0oYt6uzq@reports-tekie.postgres.database.azure.com/tekie', pool_recycle=3600);
DBConnection = AlchemyEngine.connect();
#school_data_total = pd.read_sql("select * from \"userSessionReport\" LIMIT 10", DBConnection);
#School_data_total = pd.read_sql("select \"id\",\"studentId\",\"studentName\",\"userRole\",\"studentGrade\",\"studentSection\",\"classroomId\",\"classroomTitle\",\"schoolName\",\"courseTitle\",\"sessionTitle\",\"sessionType\",\"sessionId\",\"courseCategory\",\"sessionStart\",\"sessionEnd\",\"sessionStatus\",\"studentAttendance\",\"createdAt\",\"updatedAt\",\"sessionCreationDate\",\"sessionUpdationAt\" from \"userSessionReport\"", DBConnection);
School_data_total = pd.read_sql("select \"id\",\"studentId\",\"studentName\",\"userRole\",\"studentGrade\",\"studentSection\",\"classroomId\",\"classroomTitle\",\"schoolName\",\"courseTitle\",\"sessionTitle\",\"sessionType\",\"sessionId\",\"courseCategory\",\"sessionStart\",\"sessionEnd\",\"sessionStatus\",\"studentAttendance\",\"createdAt\",\"updatedAt\" from \"userSessionReport\"", DBConnection);

#New Table updatedUserSessionReport
School_data_total = pd.read_sql("select \"id\",\"studentId\",\"studentName\",\"userRole\",\"studentGrade\",\"studentSection\",\"classroomId\",\"classroomTitle\",\"schoolName\",\"courseTitle\",\"sessionTitle\",\"sessionType\",\"sessionId\",\"teacherName\",\"courseCategory\",\"sessionStart\",\"sessionEnd\",\"sessionStatus\",\"studentAttendance\" from \"updatedUserSessionReport\"", DBConnection);
School_data_total['sessionStartDate'] = pd.to_datetime(School_data_total['sessionStart']).dt.date
#%%
import datetime
'''
['id', 'studentName', 'userRole', 'studentGrade', 'studentSection', 'classroomTitle', 'schoolName', 'sessionTitle', 'sessionType', 'courseCategory', 'sessionStart', 'sessionEnd', 'sessionStatus', 'studentName', 'studentAttendance']
'''
#%% Temp
#Batchsession_teacher_data = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRBPTBY5F_EY88KRUD5P4Th5IAxJVvdiLz_VIrENvo4AUFH_fS6K7iBA1CiaOv3GuQmV_xBXC79uP2h/pub?gid=1729540697&single=true&output=csv")
#Batchsession_teacher_data = Batchsession_teacher_data.rename(columns={'teacherName':'sessionTeacher'})
#School_data_total = School_data_total.merge(Batchsession_teacher_data,how='left',on='sessionId')
#%% Preparing all keys over the data
School_data_total['Key_1'] = School_data_total['schoolName']+School_data_total['classroomId']
School_data_total['Key_2'] = School_data_total['schoolName']+School_data_total['classroomId']+School_data_total['teacherName']
School_data_total['Key_3'] = School_data_total['schoolName']+School_data_total['classroomId']+School_data_total['teacherName']+School_data_total['courseTitle']+School_data_total['sessionTitle']
School_data_total['Key_4'] = School_data_total['schoolName']+School_data_total['classroomId']+School_data_total['teacherName']+School_data_total['sessionId']
School_data_total['Key_5'] = School_data_total['schoolName']+School_data_total['classroomId']+School_data_total['teacherName']+School_data_total['sessionId']+School_data_total['studentId']
#%% Splitting Student and Teacher Data
school_teacher_data_total = School_data_total[School_data_total['userRole']=='Teacher']
school_student_data_total = School_data_total[School_data_total['userRole']=='Student']
#%%
school_teacher_data_total = school_teacher_data_total.drop_duplicates(subset='Key_4', keep="last")
school_student_data_total = school_student_data_total.drop_duplicates(subset='Key_5', keep="last")
student_duplicate_data = school_student_data_total[school_student_data_total.duplicated('Key_5')]

#%%
School_batchsession_in_started = school_teacher_data_total[school_teacher_data_total['sessionStatus']=='started']
School_batchsession_in_started = School_batchsession_in_started.groupby('Key_2',as_index=False).agg({'sessionStatus':'count'}).rename(columns={'sessionStatus':'No. of Sessions in Started'})
School_batchsession_in_completed = school_teacher_data_total[school_teacher_data_total['sessionStatus']=='completed']
School_batchsession_in_completed = School_batchsession_in_completed.groupby('Key_2',as_index=False).agg({'sessionStatus':'count'}).rename(columns={'sessionStatus':'No. of Sessions in Completed'})

#%% 
'''
School Batch Level Overview
'''
school_batch_data_overview = all_batches_df
school_batch_data_overview = school_batch_data_overview.merge(School_batchsession_in_started,on="Key_2",how='left')
school_batch_data_overview = school_batch_data_overview.merge(School_batchsession_in_completed,on="Key_2",how='left')
school_batch_data_overview['No. of Sessions in Started'] = school_batch_data_overview['No. of Sessions in Started'].fillna(0)
school_batch_data_overview['No. of Sessions in Completed'] = school_batch_data_overview['No. of Sessions in Completed'].fillna(0)
school_batch_data_overview['Total Labs Started'] = school_batch_data_overview['No. of Sessions in Started']+school_batch_data_overview['No. of Sessions in Completed']
school_batch_data_overview['Completion %'] = (school_batch_data_overview['Total Labs Started']/school_batch_data_overview['No. of Assigned Lab Sessions'])*100


#%% Attendance %
School_attendance_overview = school_student_data_total.groupby('Key_2',as_index=False).agg({'studentAttendance':'count'}).rename(columns={'studentAttendance':'No. of Attendance Records'})
School_present_students = school_student_data_total[school_student_data_total['studentAttendance']==True]
School_present_students = School_present_students.groupby('Key_2',as_index=False).agg({'studentAttendance':'count'}).rename(columns={'studentAttendance':'No. of Present Records'})

School_attendance_overview = School_attendance_overview.merge(School_present_students,on='Key_2',how='left')
School_attendance_overview['No. of Present Records'] = School_attendance_overview['No. of Present Records'].fillna(0)
School_attendance_overview['Attendance %'] = (School_attendance_overview['No. of Present Records']/School_attendance_overview['No. of Attendance Records'])*100

school_batch_data_overview = school_batch_data_overview.merge(School_attendance_overview[['Key_2','Attendance %']],on='Key_2',how='left')
school_batch_data_overview['Attendance %'] = school_batch_data_overview['Attendance %'].fillna(0)
#%%
'''
School Level Overview
'''
school_data_overview = all_school_df
School_completion_attendance = school_batch_data_overview.groupby('School',as_index=False).agg({'School':'first','No. of Sessions in Started':'sum','No. of Sessions in Completed':'sum','Total Labs Started':'sum','Completion %':'mean','Attendance %':'mean'})
school_data_overview = school_data_overview.merge(School_completion_attendance,on='School',how='left')
#%%
School_batches_started = school_batch_data_overview[school_batch_data_overview['Total Labs Started']>0]
School_batches_started_overview = School_batches_started.groupby('School',as_index=False).agg({'Key_2':'count','Classroom Title':'unique'}).rename(columns={'Key_2':'No. of batches started','Classroom Title':'Batches Started'})

School_batches_not_started = school_batch_data_overview[school_batch_data_overview['Total Labs Started']==0]
School_batches_not_started_overview = School_batches_not_started.groupby('School',as_index=False).agg({'Key_2':'count','Classroom Title':'unique'}).rename(columns={'Key_2':'No. of not batches started','Classroom Title':'Batches not Started'})

school_data_overview = school_data_overview.merge(School_batches_started_overview,on='School',how='left')
school_data_overview = school_data_overview.merge(School_batches_not_started_overview,on='School',how='left')
#%% Batch Session Level
school_batch_session_overview = school_teacher_data_total

School_session_attendance_overview = school_student_data_total.groupby('Key_3',as_index=False).agg({'studentAttendance':'count'}).rename(columns={'studentAttendance':'No. of Attendance Records'})
School_session_present_students = school_student_data_total[school_student_data_total['studentAttendance']==True]
School_session_present_students = School_session_present_students.groupby('Key_3',as_index=False).agg({'studentAttendance':'count'}).rename(columns={'studentAttendance':'No. of Present Records'})
#%%
School_session_attendance_overview = School_session_attendance_overview.merge(School_session_present_students,on='Key_3',how='left')
School_session_attendance_overview['No. of Present Records'] = School_session_attendance_overview['No. of Present Records'].fillna(0)
School_session_attendance_overview['Attendance %'] = (School_session_attendance_overview['No. of Present Records']/School_session_attendance_overview['No. of Attendance Records'])*100

school_batch_session_overview = school_batch_session_overview.merge(School_session_attendance_overview,on='Key_3',how='left')

#%%
# sheets = ['schoolDataOverview', 'schoolBatchOverview', 'schoolBatchSessionOverview']
schoolDataOverview = school_data_overview.to_csv('schoolDataOverview.csv', index=False)
schoolBatchWiseOverview = school_batch_data_overview.to_csv('schoolBatchOverview.csv', index=False)
schoolBatchSessionWiseOverview = school_batch_session_overview.to_csv('schoolBatchSessionOverview.csv', index=False)
schoolTeacherDataOverview = school_teacher_data_total.to_csv('schoolTeacherDataOverview.csv', index=False)

sheets = [
        {
            'range': 'School Overview Data',
            'values': schoolDataOverview,
            'file': 'schoolDataOverview.csv'
                
        },
        {
            'range': 'School Batch-wise Overview',
            'values': schoolBatchWiseOverview,
            'file': 'schoolBatchOverview.csv'
                
        },
        {
            'range': 'School Batch Session-wise Overview',
            'values': schoolBatchSessionWiseOverview,
            'file': 'schoolBatchSessionOverview.csv'
                
        },
        {
            'range': 'School Teacher-wise Overview',
            'values': schoolTeacherDataOverview,
            'file': 'schoolTeacherDataOverview.csv'
                
        },
        
        
        
        ]
# remove file from sheets

sheetsData = []
for i in sheetsData: 
    sheetsData.append(i['file'])



for sheet in sheets:
  ranges = sheet['range']
  values = sheet['values']
  file = sheet['file']
  data = [{
      'range': ranges,
      'values': values
  }]
  with open(file, 'r') as f:
      
    reader = csv.reader(f)
    data = list(reader)
    body = {
      'values': sheets
      }
    print(body)

    service.spreadsheets().values().clear(
          spreadsheetId=spreadsheet_id, range=range_
    ).execute()
    service.spreadsheets().values().update(
          spreadsheetId=spreadsheet_id, range=range_,
          valueInputOption='RAW', body=body
      ).execute()