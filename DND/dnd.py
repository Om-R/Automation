import sys
from contextlib import nullcontext

import pymysql
import logging
import pymongo
import datetime
from collections.abc import MutableMapping
from collections import namedtuple

# Configure logging
logging.basicConfig(
    filename='app.log',
    filemode='w',
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Database configuration
sql_host = 'host'
sql_port = 3306
sql_user = 'user'
sql_passwd = 'password'
sql_db = 'db name'
batch_size = 500

# LK_LOAN_MASTER variables
lm_table = 'LK_LOAN_MASTER'
lm_app_id_col = 'L_APPLICATION_ID'
lm_app_email = 'L_EMAIL'
lm_app_mobileNumber = 'L_CONTACT_NO'
modifier_name = "python script for dnd request"
mongodb_uri = "mongo url"
dbName = "lkart"
applicationIds = ['LAI-122272155']

def getEmail(email):
    if not email or 'test' in email.split('@')[0]:
        return email
    emailSplit = email.split('@')
    emailSplit[0] += 'test'
    modified_email = '@'.join(emailSplit)
    logging.info(f"Modified email = {modified_email}")
    return modified_email

def getMobileNUmber(number):
    if not number or number.endswith('0000'):
        return number
    modified_number = number + "0000"
    logging.info(f"Modified mobile number = {modified_number}")
    return modified_number


def run_migration():
    db_conn = pymysql.connect(
        host=sql_host,
        port=sql_port,
        user=sql_user,
        passwd=sql_passwd,
        db=sql_db,
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with db_conn.cursor() as cursor:
            for applicationId in applicationIds:
                query = "SELECT * FROM LK_LOAN_MASTER WHERE L_APPLICATION_ID=%s"
                cursor.execute(query, (applicationId,))
                value = cursor.fetchone()

                if value:
                    original_email = value.get('L_EMAIL', '')
                    original_mobile = value.get('L_CONTACT_NO', '')

                    if value['L_STATUS_ID'] and value['L_STATUS_ID'] != "1f974f0c-62b3-11e5-988d-d6b2fad01037":
                        email = getEmail(original_email)
                        mobileNumber = getMobileNUmber(original_mobile)

                        if email != original_email or mobileNumber != original_mobile:
                            updateQuery = """
                                    UPDATE LK_LOAN_MASTER
                                    SET L_EMAIL = %s, L_CONTACT_NO = %s,
                                        MODIFIED_VERSION_TIMESTAMP = NOW(), MODIFIED_USERNAME = %s
                                    WHERE L_APPLICATION_ID=%s
                                """
                            cursor.execute(updateQuery, (email, mobileNumber, modifier_name, applicationId))
                            db_conn.commit()
                            logging.info("Updated in LK_LOAN_MASTER for " + applicationId)
                        else:
                            logging.info("No update needed in LK_LOAN_MASTER for " + applicationId)

                        # Handle LK_USER_MASTER
                        userId = value['L_USER_ID']
                        leadId = int(value['L_LIS_LEAD_ID'])

                        selectQueryUserMaster = "SELECT * FROM LK_USER_MASTER WHERE U_GUID=%s"
                        cursor.execute(selectQueryUserMaster, (userId,))
                        value1 = cursor.fetchone()

                        if value1:
                            original_email1 = value1.get('U_EMAIL', '')
                            original_mobile1 = value1.get('U_CONTACT_NUMBER', '')
                            email1 = getEmail(original_email1)
                            mobileNumber1 = getMobileNUmber(original_mobile1)

                            if email1 != original_email1 or mobileNumber1 != original_mobile1:
                                updateQuery = """
                                        UPDATE LK_USER_MASTER
                                        SET U_EMAIL = %s, U_CONTACT_NUMBER = %s, MODIFIED_USERNAME = %s,
                                            MODIFIED_VERSION_TIMESTAMP = NOW()
                                        WHERE U_GUID=%s
                                    """
                                cursor.execute(updateQuery, (email1, mobileNumber1, modifier_name, userId))
                                db_conn.commit()
                                logging.info("Updated in LK_USER_MASTER for " + applicationId)
                            else:
                                logging.info("No update needed in LK_USER_MASTER for " + applicationId)
                        else:
                            logging.info("No data found for APPLICATION_ID " + applicationId + " in user master")

                        # Handle LK_CLS_LOAN_ACCOUNT_MASTER
                        cls_application_id = value.get('L_CLS_APPLICATION_ID')
                        selectQueryClsAccountMaster = "SELECT * FROM LK_CLS_LOAN_ACCOUNT_MASTER WHERE APPLICATION_ID=%s"
                        cursor.execute(selectQueryClsAccountMaster, (cls_application_id,))
                        value2 = cursor.fetchone()

                        if value2:
                            original_email2 = value2.get('CUSTOMER_EMAIL_ID', '')
                            original_mobile2 = value2.get('CUSTOMER_PHONE', '')
                            email2 = getEmail(original_email2)
                            mobileNumber2 = getMobileNUmber(original_mobile2)

                            if email2 != original_email2 or mobileNumber2 != original_mobile2:
                                updateQuery = """
                                        UPDATE LK_CLS_LOAN_ACCOUNT_MASTER
                                        SET CUSTOMER_EMAIL_ID = %s, CUSTOMER_PHONE = %s
                                        WHERE APPLICATION_ID=%s
                                    """
                                cursor.execute(updateQuery, (email2, mobileNumber2, cls_application_id))
                                db_conn.commit()
                                logging.info(
                                    "Updated in LK_CLS_LOAN_ACCOUNT_MASTER for APPLICATION_ID " + applicationId)
                            else:
                                logging.info("No update needed in LK_CLS_LOAN_ACCOUNT_MASTER for " + applicationId)
                        else:
                            logging.info(
                                "No data found for APPLICATION_ID " + applicationId + " in LK_CLS_LOAN_ACCOUNT_MASTER")

                    #     client = pymongo.MongoClient(mongodb_uri)
                    #     db = client[dbName]
                    #     collection = db['leads']
                    #     mongoValue = collection.find_one({'leadId': leadId})
                    #
                    #     mongoEmail = ""
                    #     mongoNumber = ""
                    #
                    #     if mongoValue:
                    #         if mongoValue['primaryEmail']:
                    #             mongoEmail = getEmail(mongoValue['primaryEmail'])
                    #         if mongoValue['primaryPhone']:
                    #             mongoNumber = getMobileNUmber(mongoValue['primaryPhone'])
                    #
                    #         modifiedValue = mongoValue['modifiedBy']
                    #         modifiedValue['name'] = modifier_name
                    #
                    #         myquery = {"_id": mongoValue['_id']}
                    #         newvalues = {"$set": {"primaryEmail": mongoEmail, "primaryPhone": mongoNumber, "modifiedBy": modifiedValue}}
                    #         collection.update_one(myquery, newvalues)
                    #         logging.info("Successfully updated in lead for APPLICATION_ID " + applicationId)
                    #     else:
                    #         logging.info("No update needed in Mongo for APPLICATION_ID " + applicationId)
                    #
                    # else:
                    #     logging.info("No data found in lead for APPLICATION_ID " + applicationId)
                else:
                    logging.info("Skipping, since the status is Disbursed for APPLICATION_ID " + applicationId)
    except Exception as e:
        logging.error('Migration failed: ' + str(e))
    finally:
        db_conn.close()
        print(datetime.datetime.now())
        print("Done")
        logger.info('All done. Connection closed.')

if __name__ == '__main__':
    logger.info("Starting migration")
    run_migration()