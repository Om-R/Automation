import boto3
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime


AWS_SERVER_PUBLIC_KEY = "AWS-Public-Key"
AWS_SERVER_SECRET_KEY = "AWS-Secret-Key"
REGION_NAME = "region-name"

s3_client = boto3.client('s3',
                    aws_access_key_id=AWS_SERVER_PUBLIC_KEY,
                    aws_secret_access_key=AWS_SERVER_SECRET_KEY,
                    region_name=REGION_NAME
                    )

def create_directory(applicationId):
    folderName = applicationId.split("-")
    folderName = folderName[0]+folderName[1]
    if not os.path.exists(folderName):
        os.mkdir(folderName)
    return folderName+"/"

def create_output_directory(folderName):
    if not os.path.exists(folderName):
        os.mkdir(folderName)
    return folderName+"/"


def create_directory_with_current_date(name):
    outputFolderName = create_output_directory("output")
    splitt = name.split("-")
    if "LAI" in name:
        directory_name = ""
    else:
        directory_name = splitt[0]
    now = datetime.now()
    date_time = now.strftime("%m_%d_%Y_")
    folderName = outputFolderName+date_time+directory_name+"/"
    if not os.path.exists(folderName):
        os.mkdir(folderName)
    return folderName


# def getS3Path(applicationId,typeIds) :
#     sqlquery = """
#     SELECT D_APPLICATION_ID,D_S3_FILE_LOCATION
#     FROM lk_dms.LK_DOCUMENTS a
#     WHERE D_TYPE_ID IN %s
#       AND D_APPLICATION_ID IN %s
#       AND a.D_IS_DELETED = 0
# """ % (str(tuple(typeIds)), str(tuple(applicationId)))
#     record = []
#     print(sqlquery)
#
#     try :
#         connection = mysql.connector.connect(host='federateddb-new.lendingkart.com' ,
#                                               database='OPS' ,
#                                               user='techoncall' ,
#                                               password="]6GC9!bEyePjq:5}" ,
#                                               ssl_disabled=True)
#         if connection.is_connected():
#             db_Info = connection.get_server_info()
#             print("Connected to MySQL Server version " , db_Info)
#             cursor = connection.cursor(buffered=True)
#             cursor.execute(sqlquery)
#             record = cursor.fetchall()
#             if record is not None:
#                 print(record)
#                 return record
#             else :
#                 return -1
#     except Error as e :
#         print("Error while connecting to MySQL" , e)
#     finally :
#         # closing database connection.
#         if connection.is_connected () :
#             cursor.close ()
#             connection.close ()
#             print("MySQL connection is closed")

def getS3Path(applicationIds, typeIds):
    if not applicationIds or not typeIds:
        print("Empty applicationIds or typeIds list.")
        return []

    placeholders_app = ', '.join(['%s'] * len(applicationIds))
    placeholders_type = ', '.join(['%s'] * len(typeIds))

    sqlquery = f"""
    SELECT D_APPLICATION_ID, D_S3_FILE_LOCATION
    FROM lk_dms.LK_DOCUMENTS a
    WHERE D_TYPE_ID IN ({placeholders_type})
      AND D_APPLICATION_ID IN ({placeholders_app})
      AND a.D_IS_DELETED = 0
    """

    params = typeIds + applicationIds  # order matters
    print("Executing query:", sqlquery)
    print("With params:", params)

    try:
        connection = mysql.connector.connect(
            host='host',
            database='database',
            user='user',
            password="password",
            ssl_disabled=True
        )
        if connection.is_connected():
            db_Info = connection.server_info  # fixed deprecation warning
            print("Connected to MySQL Server version", db_Info)
            cursor = connection.cursor()
            cursor.execute(sqlquery, params)
            record = cursor.fetchall()
            if record:
                print(record)
                return record
            else:
                print("No records found.")
                return []
    except Error as e:
        print("Error while connecting to MySQL:", e)
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")



def download_from_s3(objects, uid,count,folderName):
    if(objects['KeyCount'] > 0):
        for obj in objects['Contents']:
            print (obj)
            path = obj['Key'].split('/')
            fileName = path[-2] + "." + path[-1].split(".")[-1]
            uniqueId = ""
            for u in uid:
                uniqueId+=u
            #fileName = rename_utils.renameFileIfExists(uniqueId, folderName,fileName,count, path)
            count+=1
            print("Downloading: "+obj['Key'])
            s3_client.download_file('lendingkart-secure',obj['Key'], folderName+fileName)


def start_downloadingV3(main_directory_name, listt):
    typeIds = ['KFS','SanctionLetterUnsigned','FiReport','CA_Template','DPN_Unsigned','CompleteUnsignedDRF','CPV_FI_Form','EntityDocuments','LoanAgreement','Aadhaar', 'AadhaarCard_IndAdd', 'AadhaarCard_IndAdd_redacted', 'AgriculturalBoardTrade_Reg', 'AOA','SanctionLetter',
           'CompleteSignedDRF','BackgroundVerification_DRF','AuditedFinancials_Add', 'AuditedFinancials_Reg', 'BankStatement_AddressProof', 'CableOperatorCert_Reg',
           'CentralStateGovtContractor_Reg', 'ClinicalEstablishmentCert_Reg', 'COI', 'DIR12', 'DIR3',
           'DistrictIndustriesCentre_Reg', 'DL_IndAdd', 'DrivingLicense', 'DrugLicFdDrugControlCert_Add',
           'DrugLicFdDrugControlCert_Reg', 'FactoryLicence', 'FSSAICertificate_Reg', 'GSTCertificate_Add',
           'GSTCertification_Reg', 'GumastaCertificate_Reg', 'IECLicense_Reg', 'IncomeTaxReturn_Reg',
           'LabourDeptRegistration_Add', 'LabourDeptRegistration_Reg', 'LegalMetrologyAct_Reg', 'MGT7', 'MOA',
           'MOA_AOA_COI', 'Others_BusinessAddressProof', 'Others_Ind_AddressProof', 'Others_RegistrationProof',
           'PAN_KYCBusiness', 'PAN_KYCOthers', 'PartnershipDeed', 'PartnershipFirmRegistration', 'Passport',
           'Passport_IndAdd', 'Photograph', 'ProfessionalTax_Reg', 'ProvisionalGst_Reg', 'RCMCMPGPC_Add',
           'RCunderMunicipality ', 'RegisteredRentAgreement_Add', 'RegRentAgreement_IndAdd', 'SaleDeed_Add',
           'SaleDeed_IndAdd', 'SavingBSPassbook_IndAdd', 'SebiCert_Reg', 'ShopEstablishmentAct_Add',
           'ShopEstablishmentAct_Reg', 'TaxCertificateNonGST_Add', 'TaxCertificateNonGST_Reg', 'TradeMarkCert_Reg',
           'UdyamRegistration_Reg', 'UdyogAadhaar_Reg', 'UtilityBills_Add', 'UtilityBills_IndAdd', 'UtilityBill_Reg',
           'VAT_Reg', 'VoterId', 'VoterID_IndAdd','KYC_OSV','CompleteSignedAgreement','CompleteSignedDRF',
           'OtherSigned','OthersSignedDocuments','OthersSpecial_Signed','Photograph_Signed']
    locations = getS3Path(listt,typeIds)
    count1 = 0
    for location in locations:
        appId = location[0]
        folderName = create_directory(main_directory_name+"/"+appId)
        print("Trying to get " + location[1] + "for : " + appId)
        x1 = location[1]
        objects = s3_client.list_objects_v2(Bucket='lendingkart-secure',Prefix=x1)
        uid = appId.split('-')
        try :
            download_from_s3(objects , uid , count1 , folderName)
            count1+=1
        except Exception as e :
            print("****Exeption occured**** While downloading Business Details data: %s, Exception: %s" ,
                           str(objects) , str(e))
            # list_of_failed_appId.append([appId, Indi])


if __name__ == "__main__":
    appIds = []
    main_directory_name = create_directory_with_current_date("Fresh")
    start_downloadingV3(main_directory_name, appIds)