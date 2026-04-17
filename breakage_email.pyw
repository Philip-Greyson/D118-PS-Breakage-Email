"""Script to send an email to contacts with data access when their student has a breakage recorded in MBA Device Manager+.

https://github.com/Philip-Greyson/D118-PS-Breakage-Email

Needs the google-api-python-client, google-auth-httplib2 and the google-auth-oauthlib:
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
also needs oracledb: pip install oracledb --upgrade
"""

import base64
import os  # needed for environement variable reading
import sys
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

BOOKKEEPER_EMAIL_SUFFIX = '-bookkeeper@d118.org'  # the email suffix for the bookkeepers, which is just the school abbreviation with "-bookkeeper" added to the end of it

DRY_RUN = False  # set to True to not actually send any emails, just print out the info for what would have been sent. Set to False to actually send the emails.
TEST_RUN = True  # set to True to send the emails to a specific recipient (set in the environment variable TEST_EMAIL) instead of the actual contacts, for testing purposes. Set to False to send to the actual contacts.
TEST_EMAIL = ''  # if TEST_RUN is True, this is the email that the notifications will be sent to instead of the actual contacts' emails. Should be set in an environment variable for security, but can be hardcoded for testing.

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


def send_breakage_email_to_parents(breakage_id: int, breakage_date: str, school_name: str, dev_name: str, serial: str, breakage_details: str, stu_num: int, stu_dcid: int, first_name: str, last_name: str) -> None:
    """Function to send the email about the breakage to the contacts with custody."""
    try:
        # get_data_access_contacts(stuDCID)  # get the contacts with data access
        print(f'DBUG: Getting contacts for {stu_num} - DCID {stu_dcid} for breakageID {breakage_id}')
        print(f'DBUG: Getting contacts for {stu_num} - DCID {stu_dcid} for breakageID {breakage_id}', file=log)
        # get_data_access_contacts(stuDCID)  # get the contacts with data access
        contacts_to_email = get_custody_contacts(stu_dcid)  # get the contacts with custody
        if contacts_to_email:
            for contact in contacts_to_email:
                try:
                    contact_first_last = f'{contact[0]} {contact[1]}'  # get their name in one string
                    to_email = str(contact[2])
                    print(f'INFO: Sending email to {contact_first_last} - {to_email} about breakageID {breakage_id} that happened on {breakage_date} at {school_name} to device {serial} by student {stu_num}')
                    print(f'INFO: Sending email to {contact_first_last} - {to_email} about breakageID {breakage_id} that happened on {breakage_date} at {school_name} to device {serial} by student {stu_num}', file=log)
                    # Do the email sending
                    mime_message = EmailMessage()  # create an email message object
                    # define headers
                    mime_message['To'] = to_email if not TEST_RUN else TEST_EMAIL  # if it's a test run, send to the test email instead of the actual contact's email
                    mime_message['Subject'] = f'Notification of Student Device Damage - {firstName} {lastName}'  # subject line of the email

                    # Plain-text fallback (for email clients that don't support HTML)
                    plain_text = (
                        f'Dear {contact_first_last},\n'
                        f'This email is to notify you that damage has been reported for the school-assigned device currently issued to {first_name} {last_name} at {school_name}. Our goal is to keep you informed regarding the condition of the technology provided to your student.\n\n'
                        f'Incident Details:\n'
                        f'  • Device Name: {dev_name}\n'
                        f'  • Serial Number: {serial}\n'
                        f'  • Date of Damage: {breakage_date}\n'
                        f'  • Description of Damage: {breakage_details}\n\n'
                        f'Warranty and Responsibility Information: The district-provided warranty covers standard accidental damage. However, please be aware of the following policies:\n'
                        f'  • Intentional Damage: If school administration determines that damage was caused intentionally, the cost of the repair or device replacement will be charged to your student\'s account. Should this be the case, a building administrator will be in contact with you.\n'
                        f'  • Repeated Incidents: A recurring pattern of device damage may result in additional repair fees or a review of the student\'s equipment access. If you have further questions related to this, please contact your child\'s building Technology Specialist.\n\n'
                        f'If you have any questions regarding this incident or the repair process, please contact the building\'s administration team directly.\n'
                        f'Best regards,\n'
                        f'District 118 Technology Department'
                    )

                    # HTML version with bold formatting
                    html_content = (
                        f'<p>Dear {contact_first_last},</p>'
                        f'<p>This email is to notify you that damage has been reported for the school-assigned device currently issued to <b>{first_name} {last_name}</b> at <b>{school_name}</b>. Our goal is to keep you informed regarding the condition of the technology provided to your student.</p>'
                        f'<p><b>Incident Details:</b></p>'
                        f'<ul>'
                        f'  <li><b>Device Name:</b> {dev_name}</li>'
                        f'  <li><b>Serial Number:</b> {serial}</li>'
                        f'  <li><b>Date of Damage:</b> {breakage_date}</li>'
                        f'  <li><b>Description of Damage:</b> {breakage_details}</li>'
                        f'</ul>'
                        f'<p><b>Warranty and Responsibility Information:</b> The district-provided warranty covers standard accidental damage. However, please be aware of the following policies:</p>'
                        f'<ul>'
                        f'  <li><b>Intentional Damage:</b> If school administration determines that damage was caused intentionally, the cost of the repair or device replacement will be charged to your student\'s account. Should this be the case, a building administrator will be in contact with you.</li>'
                        f'  <li><b>Repeated Incidents:</b> A recurring pattern of device damage may result in additional repair fees or a review of the student\'s equipment access. If you have further questions related to this, please contact your child\'s building Technology Specialist.</li>'
                        f'</ul>'
                        f'<p>If you have any questions regarding this incident or the repair process, please contact the building\'s administration team directly.</p>'
                        f'<p>Best regards,<br><b>District 118 Technology Department</b></p>'
                    )

                    # Set plain text as the base content, then add HTML as the preferred alternative
                    mime_message.set_content(plain_text)
                    mime_message.add_alternative(html_content, subtype='html')

                    if not DRY_RUN:  # only encode and send the email if we are not in a dry run
                        # encoded message
                        encoded_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode()
                        create_message = {'raw': encoded_message}
                        send_message = (service.users().messages().send(userId="me", body=create_message).execute())
                        print(f'DBUG: Email sent, message ID: {send_message["id"]}')  # print out resulting message Id
                        print(f'DBUG: Email sent, message ID: {send_message["id"]}', file=log)
                except HttpError as er:   # catch Google API http errors, get the specific message and reason from them for better logging
                    status = er.status_code
                    details = er.error_details[0]  # error_details returns a list with a dict inside of it, just strip it to the first dict
                    print(f'ERROR {status} from Google API while sending breakage notification email to {to_email} about broken device with breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}')
                    print(f'ERROR {status} from Google API while sending breakage notification email to {to_email} about broken device with breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}', file=log)
                except Exception as er:
                    print(f'ERROR while sending email to {to_email} about broken device with breakage ID {breakageID}: {er}')
                    print(f'ERROR while sending email to {to_email} about broken device with breakage ID {breakageID}: {er}', file=log)
    except Exception as er:
        print(f'ERROR while constructing contact list to contact for student {stu_num} about broken device with breakage ID {breakageID}: {er}')
        print(f'ERROR while constructing contact list to contact for student {stu_num} about broken device with breakage ID {breakageID}: {er}', file=log)

def send_lost_email_to_parents(breakage_id: int, breakage_date: str, school_name: str, dev_name: str, serial: str, breakage_details: str, stu_num: int, stu_dcid: int, first_name: str, last_name: str) -> None:
    """Function to send the email about the lost device to the contacts with custody."""
    try:
        # get_data_access_contacts(stuDCID)  # get the contacts with data access
        print(f'DBUG: Getting contacts for {stu_num} - DCID {stu_dcid} for breakageID {breakage_id}')
        print(f'DBUG: Getting contacts for {stu_num} - DCID {stu_dcid} for breakageID {breakage_id}', file=log)
        # get_data_access_contacts(stuDCID)  # get the contacts with data access
        contacts_to_email = get_custody_contacts(stu_dcid)  # get the contacts with custody
        if contacts_to_email:
            for contact in contacts_to_email:
                try:
                    contact_first_last = f'{contact[0]} {contact[1]}'  # get their name in one string
                    to_email = str(contact[2])
                    print(f'INFO: Sending email to {contact_first_last} - {to_email} about lost device breakageID {breakage_id} that happened on {breakage_date} at {school_name} to device {serial} by student {stu_num}')
                    print(f'INFO: Sending email to {contact_first_last} - {to_email} about lost device breakageID {breakage_id} that happened on {breakage_date} at {school_name} to device {serial} by student {stu_num}', file=log)
                    # Do the email sending
                    mime_message = EmailMessage()  # create an email message object
                    # define headers
                    mime_message['To'] = to_email if not TEST_RUN else TEST_EMAIL  # if it's a test run, send to the test email instead of the actual contact's email
                    mime_message['Subject'] = f'Important: {first_name} {last_name}\'s District 118 Chromebook Reported as Lost'  # subject line of the email

                    # Plain-text fallback (for email clients that don't support HTML)
                    plain_text = (
                        f'Dear {contact_first_last},\n'
                        f'This email is to notify you that your student {first_name} {last_name} at {school_name} has reported that their district-issued Chromebook is lost. Our goal is to keep you informed regarding the condition of the technology provided to your student.\n\n'
                        f'Next steps & Key Information:\n'
                        f'  • Review Device Details: The details for the missing device are included below. You can also log into the PowerSchool Parent Portal for more information.\n'
                        f'  • Talk to Your Student: Please speak with your student about the device to see if they can recall where it might be.\n'
                        f'  • Account Charge: A temporary charge for the lost device will be added to your child\'s PowerSchool account.\n'
                        f'  • Charge Removal: If the device is found, the charge will be immediately removed from the account.\n'
                        f'  • Questions: If you have any questions, please contact your child\'s school directly.\n\n'
                        f'Lost Device Details:\n'
                        f'  • Device Name: {dev_name}\n'
                        f'  • Serial Number: {serial}\n'
                        f'  • Date Reported Lost: {breakage_date}\n'
                        f'  • Details: {breakage_details}\n\n'
                        f'Best regards,\n'
                        f'District 118 Technology Department'
                    )

                    # HTML version with bold formatting
                    html_content = (
                        f'<p>Dear {contact_first_last},</p>'
                        f'<p>This email is to notify you that your student {first_name} {last_name} at {school_name} has reported that their district-issued Chromebook is lost. Our goal is to keep you informed regarding the condition of the technology provided to your student.</p>'
                        f'<p><b>Next steps & Key Information:</b></p>'
                        f'<ul>'
                        f'  <li><b>Review Device Details:</b> The details for the missing device are included below. You can also log into the PowerSchool Parent Portal for more information.</li>'
                        f'  <li><b>Talk to Your Student:</b> Please speak with your student about the device to see if they can recall where it might be.</li>'
                        f'  <li><b>Account Charge:</b> A temporary charge for the lost device will be added to your child\'s PowerSchool account.</li>'
                        f'  <li><b>Charge Removal:</b> If the device is found, the charge will be immediately removed from the account.</li>'
                        f'  <li><b>Questions:</b> If you have any questions, please contact your child\'s school directly.</li>'
                        f'</ul>'
                        f'<p><b>Lost Device Details:</b></p>'
                        f'<ul>'
                        f'  <li><b>Device Name:</b> {dev_name}</li>'
                        f'  <li><b>Serial Number:</b> {serial}</li>'
                        f'  <li><b>Date Reported Lost:</b> {breakage_date}</li>'
                        f'  <li><b>Details:</b> {breakage_details}</li>'
                        f'</ul>'
                        f'<p>Best regards,<br><b>District 118 Technology Department</b></p>'
                    )

                    # Set plain text as the base content, then add HTML as the preferred alternative
                    mime_message.set_content(plain_text)
                    mime_message.add_alternative(html_content, subtype='html')

                    if not DRY_RUN:  # only encode and send the email if we are not in a dry run
                        # encoded message
                        encoded_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode()
                        create_message = {'raw': encoded_message}
                        send_message = (service.users().messages().send(userId="me", body=create_message).execute())
                        print(f'DBUG: Email sent, message ID: {send_message["id"]}')  # print out resulting message Id
                        print(f'DBUG: Email sent, message ID: {send_message["id"]}', file=log)
                except HttpError as er:   # catch Google API http errors, get the specific message and reason from them for better logging
                    status = er.status_code
                    details = er.error_details[0]  # error_details returns a list with a dict inside of it, just strip it to the first dict
                    print(f'ERROR {status} from Google API while sending breakage notification email to {to_email} about lost device with breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}')
                    print(f'ERROR {status} from Google API while sending breakage notification email to {to_email} about lost device with breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}', file=log)
                except Exception as er:
                    print(f'ERROR while sending email to {to_email} about lost device with breakage ID {breakageID}: {er}')
                    print(f'ERROR while sending email to {to_email} about lost device with breakage ID {breakageID}: {er}', file=log)
    except Exception as er:
        print(f'ERROR while constructing contact list to contact for student {stu_num} about lost device with breakage ID {breakageID}: {er}')
        print(f'ERROR while constructing contact list to contact for student {stu_num} about lost device with breakage ID {breakageID}: {er}', file=log)

def send_stolen_email_to_parents(breakage_id: int, breakage_date: str, school_name: str, dev_name: str, serial: str, stu_num: int, stu_dcid: int, first_name: str, last_name: str) -> None:
    """Function to send the email about the stolen device to the contacts with custody."""
    try:
        # get_data_access_contacts(stuDCID)  # get the contacts with data access
        print(f'DBUG: Getting contacts for {stu_num} - DCID {stu_dcid} for breakageID {breakage_id}')
        print(f'DBUG: Getting contacts for {stu_num} - DCID {stu_dcid} for breakageID {breakage_id}', file=log)
        # get_data_access_contacts(stuDCID)  # get the contacts with data access
        contacts_to_email = get_custody_contacts(stu_dcid)  # get the contacts with custody
        if contacts_to_email:
            for contact in contacts_to_email:
                try:
                    contact_first_last = f'{contact[0]} {contact[1]}'  # get their name in one string
                    to_email = str(contact[2])
                    print(f'INFO: Sending email to {contact_first_last} - {to_email} about stolen device breakageID {breakage_id} that happened on {breakage_date} at {school_name} to device {serial} by student {stu_num}')
                    print(f'INFO: Sending email to {contact_first_last} - {to_email} about stolen device breakageID {breakage_id} that happened on {breakage_date} at {school_name} to device {serial} by student {stu_num}', file=log)
                    # Do the email sending
                    mime_message = EmailMessage()  # create an email message object
                    # define headers
                    mime_message['To'] = to_email if not TEST_RUN else TEST_EMAIL  # if it's a test run, send to the test email instead of the actual contact's email
                    mime_message['Subject'] = f'Important: {first_name} {last_name}\'s District 118 Chromebook Reported as Stolen'  # subject line of the email

                    # Plain-text fallback (for email clients that don't support HTML)
                    plain_text = (
                        f'Dear {contact_first_last},\n'
                        f'This email is to notify you that your student {first_name} {last_name} at {school_name} has reported that their district-issued Chromebook was stolen. Our goal is to keep you informed regarding the condition of the technology provided to your student.\n\n'
                        f'Next steps & Key Information:\n'
                        f'  • Review Device Details: The details for the missing device are included below. You can also log into the PowerSchool Parent Portal for more information.\n'
                        f'  • Talk to Your Student: Please speak with your student to discuss the circumstances of the device being stolen.\n'
                        f'  • Investigation: The administrative team at your child\'s school will investigate this claim.\n'
                        f'  • Follow-Up: We will contact you with further details once the investigation is complete.\n\n'
                        f'Lost Device Details:\n'
                        f'  • Device Name: {dev_name}\n'
                        f'  • Serial Number: {serial}\n'
                        f'  • Date Reported Stolen: {breakage_date}\n\n'
                        f'Best regards,\n'
                        f'District 118 Technology Department'
                    )

                    # HTML version with bold formatting
                    html_content = (
                        f'<p>Dear {contact_first_last},</p>'
                        f'<p>This email is to notify you that your student {first_name} {last_name} at {school_name} has reported that their district-issued Chromebook was stolen. Our goal is to keep you informed regarding the condition of the technology provided to your student.</p>'
                        f'<p><b>Next steps & Key Information:</b></p>'
                        f'<ul>'
                        f'  <li><b>Review Device Details:</b> The details for the missing device are included below. You can also log into the PowerSchool Parent Portal for more information.</li>'
                        f'  <li><b>Talk to Your Student:</b> Please speak with your student to discuss the circumstances of the device being stolen.</li>'
                        f'  <li><b>Investigation:</b> The administrative team at your child\'s school will investigate this claim.</li>'
                        f'  <li><b>Follow-Up:</b> We will contact you with further details once the investigation is complete.</li>'
                        f'</ul>'
                        f'<p><b>Lost Device Details:</b></p>'
                        f'<ul>'
                        f'  <li><b>Device Name:</b> {dev_name}</li>'
                        f'  <li><b>Serial Number:</b> {serial}</li>'
                        f'  <li><b>Date Reported Stolen:</b> {breakage_date}</li>'
                        f'</ul>'
                        f'<p>Best regards,<br><b>District 118 Technology Department</b></p>'
                    )

                    # Set plain text as the base content, then add HTML as the preferred alternative
                    mime_message.set_content(plain_text)
                    mime_message.add_alternative(html_content, subtype='html')

                    if not DRY_RUN:  # only encode and send the email if we are not in a dry run
                        # encoded message
                        encoded_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode()
                        create_message = {'raw': encoded_message}
                        send_message = (service.users().messages().send(userId="me", body=create_message).execute())
                        print(f'DBUG: Email sent, message ID: {send_message["id"]}')  # print out resulting message Id
                        print(f'DBUG: Email sent, message ID: {send_message["id"]}', file=log)
                except HttpError as er:   # catch Google API http errors, get the specific message and reason from them for better logging
                    status = er.status_code
                    details = er.error_details[0]  # error_details returns a list with a dict inside of it, just strip it to the first dict
                    print(f'ERROR {status} from Google API while sending breakage notification email to {to_email} about stolen device with breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}')
                    print(f'ERROR {status} from Google API while sending breakage notification email to {to_email} about stolen device with breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}', file=log)
                except Exception as er:
                    print(f'ERROR while sending email to {to_email} about stolen device with breakage ID {breakageID}: {er}')
                    print(f'ERROR while sending email to {to_email} about stolen device with breakage ID {breakageID}: {er}', file=log)
    except Exception as er:
        print(f'ERROR while constructing contact list to contact for student {stu_num} about stolen device with breakage ID {breakageID}: {er}')
        print(f'ERROR while constructing contact list to contact for student {stu_num} about stolen device with breakage ID {breakageID}: {er}', file=log)

def send_lost_email_to_bookkeepers(school_abbreviation: str, breakage_date: str, stu_num: int, first_name: str, last_name: str) -> None:
    """Function to send the email about the lost device to the bookkeepers."""
    to_email = f"{school_abbreviation}{BOOKKEEPER_EMAIL_SUFFIX}"
    print(f'INFO: Sending email to bookkeeper at {to_email} about lost device for student {stu_num} - {first_name} {last_name} that happened on {breakage_date}.')
    print(f'INFO: Sending email to bookkeeper at {to_email} about lost device for student {stu_num} - {first_name} {last_name} that happened on {breakage_date}.', file=log)
    try:
        # Do the email sending
        mime_message = EmailMessage()  # create an email message object
        # define headers
        mime_message['To'] = to_email if not TEST_RUN else TEST_EMAIL  # if it's a test run, send to the test email instead of the actual contact's email
        mime_message['Subject'] = f'Charge Needed: {first_name} {last_name}\'s District 118 Chromebook Reported as Lost'  # subject line of the email

        # Plain-text fallback (for email clients that don't support HTML)
        plain_text = (
            f'Dear {school_abbreviation} Bookkeeper,\n'
            f'Student {stu_num} - {first_name} {last_name} reported their district-issued Chromebook as lost on {breakage_date}.\n'
            f'Please add a charge to their account of $351 for the lost device.\n\n'
            f'If you have further questions, please reach out to your building technology specialist and your administration team.\n\n'
            f'Best regards,\n'
            f'District 118 Technology Department'
        )

        # HTML version with bold formatting
        html_content = (
            f'<p>Dear {school_abbreviation} Bookkeeper,</p>'
            f'<p>Student {stu_num} - {first_name} {last_name} reported their district-issued Chromebook as lost on {breakage_date}.</p>'
            f'<p>Please add a charge to their account of <b>$351</b> for the lost device.</p>'
            f'<p>If you have further questions, please reach out to your building technology specialist and your administration team.</p>'
            f'<p>Best regards,<br><b>District 118 Technology Department</b></p>'
        )

        # Set plain text as the base content, then add HTML as the preferred alternative
        mime_message.set_content(plain_text)
        mime_message.add_alternative(html_content, subtype='html')

        # encoded message
        if not DRY_RUN:  # only encode and send the email if we are not in a dry run
            encoded_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode()
            create_message = {'raw': encoded_message}
            send_message = (service.users().messages().send(userId="me", body=create_message).execute())
            print(f'DBUG: Email sent, message ID: {send_message["id"]}')  # print out resulting message Id
            print(f'DBUG: Email sent, message ID: {send_message["id"]}', file=log)
    except HttpError as er:   # catch Google API http errors, get the specific message and reason from them for better logging
        status = er.status_code
        details = er.error_details[0]  # error_details returns a list with a dict inside of it, just strip it to the first dict
        print(f'ERROR {status} from Google API while sending breakage notification email to {to_email} about breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}')
        print(f'ERROR {status} from Google API while sending breakage notification email to {to_email} about breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}', file=log)
    except Exception as er:
        print(f'ERROR while sending email to {to_email} about breakage ID {breakageID}: {er}')
        print(f'ERROR while sending email to {to_email} about breakage ID {breakageID}: {er}', file=log)

def send_stolen_email_to_admins(school_abbreviation: str, stu_num: int, first_name: str, last_name: str, breakage_date: str, stu_dcid: int) -> None:
    """Function to send the email about the stolen device to the principals."""
    # start by getting the principal and assistant principals emails for the school based on the abbreviation
    cur.execute('SELECT principal, principalemail, asstprincipal, asstprincipalemail FROM schools WHERE abbreviation = :school', school=school_abbreviation)
    admins = cur.fetchall()
    principal_name = str(admins[0][0]) if admins[0][0] else f'{school_abbreviation} Principal'
    principal_email = str(admins[0][1]) if admins[0][1] else ''
    asst_principal_name = str(admins[0][2]) if admins[0][2] else f'{school_abbreviation} Assistant Principal'
    asst_principal_email = str(admins[0][3]) if admins[0][3] else ''
    to_email = f'{principal_email},{asst_principal_email}'
    to_names = f'{principal_name} and {asst_principal_name}'
    if school_abbreviation == 'WHS':  # for WHS, we also want to send to the dean
        cur.execute('SELECT custom_deans_house, custom_deans_house_email FROM u_studentsuserfields WHERE studentsdcid = :dcid', dcid=stu_dcid)
        dean_info = cur.fetchall()[0]
        dean_name = dean_info[0]
        dean_email = dean_info[1]
        to_email = f'{to_email},{dean_email}'  # add the dean to the email list
        to_names = f'{to_names} and {dean_name}'  # add the dean to the names list for the email greeting
    print(f'INFO: Sending email to {school_abbreviation} admins at {to_email} about stolen device for student {stu_num} - {first_name} {last_name} that happened on {breakage_date}.')
    print(f'INFO: Sending email to {school_abbreviation} admins at {to_email} about stolen device for student {stu_num} - {first_name} {last_name} that happened on {breakage_date}.', file=log)
    try:
        # Do the email sending
        mime_message = EmailMessage()  # create an email message object
        # define headers
        mime_message['To'] = to_email if not TEST_RUN else TEST_EMAIL  # if it's a test run, send to the test email instead of the actual contact's email
        mime_message['Subject'] = f'Investigation Needed: {first_name} {last_name}\'s District 118 Chromebook Reported as Stolen'  # subject line of the email

        # Plain-text fallback (for email clients that don't support HTML)
        plain_text = (
            f'Dear {to_names},\n'
            f'Student {stu_num} - {first_name} {last_name} reported their district-issued Chromebook as stolen on {breakage_date}.\n'
            f'The student has not yet been charged for a replacement pending an investigation. Please work with the student and your building\'s technology specialist to attempt to track this device down.\n'
            f'At the conclusion of the investigation, please follow up with the technology specialist to ensure the correct devices are assigned and active for the student.\n\n'
            f'Best regards,\n'
            f'District 118 Technology Department'
        )

        # HTML version with bold formatting
        html_content = (
            f'<p>Dear {to_names},</p>'
            f'<p>Student {stu_num} - {first_name} {last_name} reported their district-issued Chromebook as stolen on {breakage_date}.</p>'
            f'<p>The student has not yet been charged for a replacement pending an investigation. Please work with the student and your building\'s technology specialist to attempt to track this device down.</p>'
            f'<p>At the conclusion of the investigation, please follow up with the technology specialist to ensure the correct devices are assigned and active for the student.</p>'
            f'<p>Best regards,<br><b>District 118 Technology Department</b></p>'
        )

        # Set plain text as the base content, then add HTML as the preferred alternative
        mime_message.set_content(plain_text)
        mime_message.add_alternative(html_content, subtype='html')

        # encoded message
        if not DRY_RUN:  # only encode and send the email if we are not in a dry run
            encoded_message = base64.urlsafe_b64encode(mime_message.as_bytes()).decode()
            create_message = {'raw': encoded_message}
            send_message = (service.users().messages().send(userId="me", body=create_message).execute())
            print(f'DBUG: Email sent, message ID: {send_message["id"]}')  # print out resulting message Id
            print(f'DBUG: Email sent, message ID: {send_message["id"]}', file=log)
    except HttpError as er:   # catch Google API http errors, get the specific message and reason from them for better logging
        status = er.status_code
        details = er.error_details[0]  # error_details returns a list with a dict inside of it, just strip it to the first dict
        print(f'ERROR {status} from Google API while sending breakage notification email to {to_email} about breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}')
        print(f'ERROR {status} from Google API while sending breakage notification email to {to_email} about breakage ID {breakageID}: {details["message"]}. Reason: {details["reason"]}', file=log)
    except Exception as er:
        print(f'ERROR while sending email to {to_email} about breakage ID {breakageID}: {er}')
        print(f'ERROR while sending email to {to_email} about breakage ID {breakageID}: {er}', file=log)

if __name__ == '__main__':
    with open('breakage_email_log.txt', 'w') as log:
        startTime = dt.now()
        startTime = startTime.strftime('%H:%M:%S')
        print(f'INFO: Execution started at {startTime}')
        print(f'INFO: Execution started at {startTime}', file=log)
        try:
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
        except Exception as er:
            print(f'ERROR while getting Google API credentials or creating service to send emails: {er}')
            print(f'ERROR while getting Google API credentials or creating service to send emails: {er}', file=log)
            sys.exit(1)  # end the execution with code 1 as we wont be able to send any emails

        # create the connecton to the PowerSchool database
        try:
            with oracledb.connect(user=DB_UN, password=DB_PW, dsn=DB_CS) as con:
                with con.cursor() as cur:  # start an entry cursor
                    print(f'INFO: Connection established to PS database on version: {con.version}')
                    print(f'INFO: Connection established to PS database on version: {con.version}', file=log)
                    with con.cursor() as cur:  # start an entry cursor
                        today = dt.now()  # get current date and time
                        searchDate = today - timedelta(days=DAYS_TO_SEARCH_BACK)  # set the start date we will compare to the breakage entry date by subtracting our timeframe from the current date
                        cur.execute('SELECT s.dcid, s.student_number, s.first_name, s.last_name, br.breakage_details, br.breakage_date, br.id, dev.device_name, dev.serial_number, schools.name, dev_status.name, schools.abbreviation FROM u_mba_device_breakage br LEFT JOIN students s ON br.studentid = s.id LEFT JOIN schools ON s.schoolid = schools.school_number LEFT JOIN u_mba_device dev ON br.deviceid = dev.id LEFT JOIN u_mba_device_status dev_status ON dev.device_statusid = dev_status.id WHERE br.whencreated >= :startdate AND br.studentid IS NOT NULL', startdate=searchDate)
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
                                breakageDate = breakage[5].strftime('%m/%d/%Y')
                                breakageID = int(breakage[6])
                                devName = str(breakage[7])
                                serial = str(breakage[8])
                                schoolName = str(breakage[9])
                                deviceStatus = str(breakage[10])
                                schoolAbbrev = str(breakage[11])
                                print(f'INFO: Processing breakage ID {breakageID} for student {stuNum} - {firstName} {lastName} at {schoolName} for device {devName} with serial {serial} that happened on {breakageDate}. Current device status is "{deviceStatus}"')
                                if deviceStatus == 'Broken/Needs Repair':
                                    print(f'DBUG: Device status is {deviceStatus}, sending email to contacts with custody')
                                    print(f'DBUG: Device status is {deviceStatus}, sending email to contacts with custody', file=log)
                                    send_breakage_email_to_parents(breakageID, breakageDate, schoolName, devName, serial, breakageDetails, stuNum, stuDCID, firstName, lastName)
                                elif deviceStatus == 'Lost':
                                    print(f'DBUG: Device status is {deviceStatus}, sending email to contacts with custody and bookkeepers')
                                    print(f'DBUG: Device status is {deviceStatus}, sending email to contacts with custody and bookkeepers', file=log)
                                    send_lost_email_to_parents(breakageID, breakageDate, schoolName, devName, serial, breakageDetails, stuNum, stuDCID, firstName, lastName)  # send the email to the contacts with custody including all the details
                                    send_lost_email_to_bookkeepers(schoolAbbrev, breakageDate, stuNum, firstName, lastName)  # send the email to the bookkeeper to notify them of the lost device and charge needed
                                elif deviceStatus == 'Stolen':
                                    print(f'DBUG: Device status is {deviceStatus}, sending email to contacts with custody and admins')
                                    print(f'DBUG: Device status is {deviceStatus}, sending email to contacts with custody and admins', file=log)
                                    send_stolen_email_to_parents(breakageID, breakageDate, schoolName, devName, serial, stuNum, stuDCID, firstName, lastName)  # send the email to the contacts with custody
                                    send_stolen_email_to_admins(schoolAbbrev, stuNum, firstName, lastName, breakageDate, stuDCID)  # send the email to the principals to notify them of the stolen device and investigation needed

                            except Exception as er:
                                print(f'ERROR while processing breakage {breakage[6]}: {er}')
                                print(f'ERROR while processing breakage {breakage[6]}: {er}', file=log)
        except Exception as er:
            print(f'ERROR while doing intial PowerSchool connection or query: {er}')
            print(f'ERROR while doing intial PowerSchool connection or query: {er}', file=log)

        endtime = dt.now()
        endtime = endtime.strftime('%H:%M:%S')
        print(f'INFO: Execution ended at {endtime}')
        print(f'INFO: Execution ended at {endtime}', file=log)
