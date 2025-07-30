"""Script to send an email to contacts with data access when their student has a breakage recorded in MBA Device Manager+.

https://github.com/Philip-Greyson/D118-PS-Breakage-Email

Needs the google-api-python-client, google-auth-httplib2 and the google-auth-oauthlib:
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
also needs oracledb: pip install oracledb --upgrade
"""

import base64
import os  # needed for environement variable reading
from datetime import datetime as dt
from datetime import timedelta
from email.message import EmailMessage

# importing module
import oracledb  # needed for connection to PowerSchool server (ordcle database)
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# setup db connection
DB_UN = os.environ.get('POWERSCHOOL_READ_USER')  # username for read-only database user
DB_PW = os.environ.get('POWERSCHOOL_DB_PASSWORD')  # the password for the database account
DB_CS = os.environ.get('POWERSCHOOL_PROD_DB')  # the IP address, port, and database name to connect to
print(f'DBUG: Database Username: {DB_UN} |Password: {DB_PW} |Server: {DB_CS}')  # debug so we can see where oracle is trying to connect to/with

DAYS_TO_SEARCH_BACK = 1  # set the number of days ago to search for breakages. If you run it every day, it should be set to 1. If it is every week, 7, etc.

# Google API Scopes that will be used. If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.compose']


def get_data_access_contacts(student_dcid: int) -> list:
    """Function to take a student DCID number, and return a dictionary of the contacts with data access names and emails."""
    cur.execute('SELECT p.firstname, p.lastname, email.emailaddress FROM guardianstudent gs \
                LEFT JOIN guardianpersonassoc gpa ON gs.guardianid = gpa.guardianid \
                LEFT JOIN personemailaddressassoc pemail ON pemail.personid = gpa.personid \
                LEFT JOIN emailaddress email ON email.emailaddressid = pemail.emailaddressid \
                LEFT JOIN person p ON gpa.personid = p.id \
                WHERE gs.studentsdcid = :dcid AND pemail.isprimaryemailaddress = 1', dcid=student_dcid)
    guardians = cur.fetchall()
    print(f'DBUG: Number of guardians with data access and current emails for DCID {student_dcid}: {len(guardians)} - {guardians}')
    print(f'DBUG: Number of guardians with data access and current emails for DCID {student_dcid}: {len(guardians)} - {guardians}', file=log)
    return guardians if len(guardians) > 0 else None  # if we had results, return the list of tuples, otherwise just return None

def get_custody_contacts(student_dcid:int) -> list:
    """Function to take a student DCID number and return dictionary of the contacts with custody names and emails."""
    cur.execute('SELECT p.firstname, p.lastname, email.emailaddress FROM studentcontactassoc sca \
                LEFT JOIN studentcontactdetail scd ON scd.studentcontactassocid = sca.studentcontactassocid \
                LEFT JOIN personemailaddressassoc pemail ON pemail.personid = sca.personid \
                LEFT JOIN emailaddress email ON email.emailaddressid = pemail.emailaddressid \
                LEFT JOIN person p ON sca.personid = p.id \
                WHERE sca.studentdcid = :dcid AND scd.isactive = 1 AND scd.iscustodial = 1 AND pemail.isprimaryemailaddress = 1', dcid=student_dcid)
    custodians = cur.fetchall()
    print(f'DBUG: Number of contacts with custody and current emails for DCID {student_dcid}: {len(custodians)} - {custodians}')
    print(f'DBUG: Number of contacts with custody and current emails for DCID {student_dcid}: {len(custodians)} - {custodians}', file=log)
    return custodians if len(custodians) > 0 else None  # if we had results, return the list of tuples, otherwise just return None


if __name__ == '__main__':
    with open('breakage_email_log.txt', 'w') as log:
        startTime = dt.now()
        startTime = startTime.strftime('%H:%M:%S')
        print(f'INFO: Execution started at {startTime}')
        print(f'INFO: Execution started at {startTime}', file=log)
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        service = build('gmail', 'v1', credentials=creds)  # create the Google API service with just gmail functionality

        # create the connecton to the PowerSchool database
        with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con:
            with con.cursor() as cur:  # start an entry cursor
                print(f'INFO: Connection established to PS database on version: {con.version}')
                print(f'INFO: Connection established to PS database on version: {con.version}', file=log)
                with con.cursor() as cur:  # start an entry cursor
                    today = dt.now()  # get current date and time
                    searchDate = today - timedelta(days=DAYS_TO_SEARCH_BACK)  # set the start date we will compare to the breakage entry date by subtracting our timeframe from the current date
                    cur.execute('SELECT s.dcid, s.student_number, s.first_name, s.last_name, br.breakage_details, br.breakage_date, br.id FROM u_mba_device_breakage br LEFT JOIN students s ON br.studentid = s.id WHERE br.whencreated >= :startdate', startdate=searchDate)
                    breakages = cur.fetchall()
                    for breakage in breakages:
                        try:
                            # print(breakage)  # debug
                            # print(breakage, file=log)  # debug
                            stuDCID = int(breakage[0])
                            stuNum = int(breakage[1])
                            firstName = str(breakage[2]).title()  # convert names from all caps to normal
                            lastName = str(breakage[3]).title()
                            breakageDetails = str(breakage[4])
                            breakageDate = breakage[5].strftime('%-m/%-d/%Y')
                            breakageID = int(breakage[6])
                            # get_data_access_contacts(stuDCID)  # get the contacts with data access
                            print(f'DBUG: Getting contacts for {stuNum} - DCID {stuDCID} for breakageID {breakageID}')
                            print(f'DBUG: Getting contacts for {stuNum} - DCID {stuDCID} for breakageID {breakageID}', file=log)
                            contactsToEmail = get_custody_contacts(stuDCID)  # get the contacts with custody
                            if contactsToEmail:
                                for contact in contactsToEmail:
                                    try:
                                        contactFirstLast = f'{contact[0]} {contact[1]}'  # get their name in one string
                                        toEmail = str(contact[2])
                                        print(f'INFO: Sending email to {contactFirstLast} - {toEmail} about breakageID {breakageID} that happened on {breakageDate}')
                                        print(f'INFO: Sending email to {contactFirstLast} - {toEmail} about breakageID {breakageID} that happened on {breakageDate}', file=log)
                                        # Do the email sending
                                        mime_message = EmailMessage()  # create an email message object
                                        # define headers
                                        mime_message['To'] = toEmail
                                        mime_message['Subject'] = f'Chromebook Breakage for Student {stuNum} on {breakageDate}'  # subject line of the email
                                        mime_message.set_content(f'Hello {contactFirstLast},\nThis email is to inform you of a device breakage that occured on {breakageDate} by your student {firstName} {lastName} - {stuNum}.\nThe details of the breakage are: {breakageDetails}\n\nPlease contact your building\'s administration team if you have any questions or concerns.')  # body of the email
                                        # encoded message
                                        encoded_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode()
                                        create_message = {'raw': encoded_message}
                                        send_message = (service.users().messages().send(userId="me", body=create_message).execute())
                                        print(f'DBUG: Email sent, message ID: {send_message["id"]}')  # print out resulting message Id
                                        print(f'DBUG: Email sent, message ID: {send_message["id"]}', file=log)
                                    except HttpError as er:   # catch Google API http errors, get the specific message and reason from them for better logging
                                        status = er.status_code
                                        details = er.error_details[0]  # error_details returns a list with a dict inside of it, just strip it to the first dict
                                        print(f'ERROR {status} from Google API while sending breakage notification email to {contact[3]} about breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}')
                                        print(f'ERROR {status} from Google API while sending breakage notification email to {contact[3]} about breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}', file=log)
                                    except Exception as er:
                                        print(f'ERROR while sending email to {contact[3]} about breakage ID {breakageID}: {er}')
                                        print(f'ERROR while sending email to {contact[3]} about breakage ID {breakageID}: {er}', file=log)
                        except Exception as er:
                            print(f'ERROR while processing breakage {breakage[5]}: {er}')
                            print(f'ERROR while processing breakage {breakage[5]}: {er}', file=log)
