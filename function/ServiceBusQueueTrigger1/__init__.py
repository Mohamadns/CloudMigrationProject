import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    #connection to database
    conn = psycopg2.connect(
            host="migprojdbserver.postgres.database.azure.com",
            database="techconfdb",
            user="mkn@migprojdbserver",
            password="Pass1234")

    try:
        # notification message and subject from database using the notification_id
        cur = conn.cursor()
        cur.execute("SELECT subject, message FROM Notification WHERE id = %s",notification_id)
        subject, body = cur.fetchone()

        # attendees email and name
    
        cur.execute("SELECT first_name, email FROM Attendee")
        attendees = cur.fetchall() 

        # Looping through each attendee and send an email with a personalized subject
        notifiedAttendee = 0
        for attendee in attendees:
            Mail(
                from_email = "info@techconf.com",
                to_emails = attendee[0],
                subject = f'{attendee[1]}: {subject}',
                plain_text_content = body
            )
            notifiedAttendee += 1

        #Updating the notification table by setting the completed date and updating the status with the total number of attendees notified
        date = datetime.utcnow()
        status = 'Notified {} attendees'.format(notifiedAttendee)
        cur.execute("UPDATE notification SET status = '{}', completed_date = '{}' WHERE id={}".format(status, date, notification_id))
        conn.commit()
        
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    # Close connection
    finally:
        if conn is not None:
            cur.close()
            conn.close()
            print('Database connection closed.')
    