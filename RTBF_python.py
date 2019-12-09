import pandas as pd
import psycopg2
import os
from datetime import datetime
import getpass
import time

# Path configuration
homedir = os.path.expanduser('~')
os.chdir(str(homedir) + '/Desktop')

# Set search term variables
first_name = 'none'
last_name = 'none'
email = 'chris.hainfeld@me.com'

# Connecting to Datawarehouse
def connect_dwh():
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(user = 'girish.tulabandu',
                                host = 'dwh-live.fundingcircle.de',
                                port = 5432,
                                password = 'Work@54321',
                                dbname = 'fundingcircle_ce_dwh')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn

# RTBF Query List
table = 'hbs.user_attributes'
query_01 = "SELECT 01 AS query_num, '{}' AS table, 'first_name' AS pii_field_01, first_name AS pii_field_01_value, 'last_name' AS pii_field_02, last_name AS pii_field_02_value, 'user_dwh_id' AS row_id_field, user_dwh_id::varchar AS row_id_field_value FROM {} WHERE first_name ILIKE '{}' AND last_name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'mess_around.list_preparation'
query_02 = "SELECT 02 AS query_num, '{}' AS table, 'first_name' AS pii_field_01, first_name AS pii_field_01_value, 'last_name' AS pii_field_02, last_name AS pii_field_02_value, 'prospect_id' AS row_id_field, prospect_id::varchar AS row_id_field_value FROM {} WHERE first_name ILIKE '{}' AND last_name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'mongo.schufa_payout_notification'
query_03 = "SELECT 03 AS query_num, '{}' AS table, 'name' AS pii_field_01, name AS pii_field_01_value, 'family_name' AS pii_field_02, family_name AS pii_field_02_value, 'mongo_id' AS row_id_field, mongo_id::varchar AS row_id_field_value FROM {} WHERE name ILIKE '{}' AND family_name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'ods.core_addresses'
query_04 = "SELECT 04 AS query_num, '{}' AS table, 'name' AS pii_field_01, name AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'dwh_id' AS row_id_field, dwh_id::varchar AS row_id_field_value FROM {} WHERE name ILIKE '{}' AND name ILIKE '{}'".format(str(table), table, first_name, last_name)
query_05 = "SELECT 05 AS query_num, '{}' AS table, 'name' AS pii_field_01, name AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'dwh_id' AS row_id_field, dwh_id::varchar AS row_id_field_value FROM {} WHERE name ILIKE '{}' AND name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'ods.core_bank_accounts'
query_06 = "SELECT 06 AS query_num, '{}' AS table, 'account_holder' AS pii_field_01, account_holder AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'dwh_id' AS row_id_field, dwh_id::varchar AS row_id_field_value FROM {} WHERE account_holder ILIKE '{}' AND account_holder ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'ods.core_bank_responses'
query_07 = "SELECT 07 AS query_num, '{}' AS table, 'debtor_account_holder' AS pii_field_01, debtor_account_holder AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'dwh_id' AS row_id_field, dwh_id::varchar AS row_id_field_value FROM {} WHERE debtor_account_holder ILIKE '{}' AND debtor_account_holder ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'ods.core_bank_transactions'
query_08 = "SELECT 08 AS query_num, '{}' AS table, 'creditor_name' AS pii_field_01, creditor_name AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'dwh_id' AS row_id_field, dwh_id::varchar AS row_id_field_value FROM {} WHERE creditor_name ILIKE '{}' AND creditor_name ILIKE '{}'".format(str(table), table, first_name, last_name)
query_09 = "SELECT 09 AS query_num, '{}' AS table, 'creditor_name' AS pii_field_01, creditor_name AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'dwh_id' AS row_id_field, dwh_id::varchar AS row_id_field_value FROM {} WHERE debtor_name ILIKE '{}' AND debtor_name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'ods.core_messages'
query_10 = "SELECT 10 AS query_num, '{}' AS table, 'receiver' AS pii_field_01, receiver AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'dwh_id' AS row_id_field, dwh_id::varchar AS row_id_field_value FROM {} WHERE receiver ILIKE '{}'".format(str(table), table, email)
query_11 = "SELECT 11 AS query_num, '{}' AS table, 'sender' AS pii_field_01, sender AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'dwh_id' AS row_id_field, dwh_id::varchar AS row_id_field_value FROM {} WHERE sender ILIKE '{}'".format(str(table), table, email)

table = 'ods.core_persons'
query_12 = "SELECT 12 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'dwh_id' AS row_id_field, dwh_id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)
query_13 = "SELECT 13 AS query_num, '{}' AS table, 'given_name' AS pii_field_01, given_name AS pii_field_01_value, 'family_name' AS pii_field_02, family_name AS pii_field_02_value, 'dwh_id' AS row_id_field, dwh_id::varchar AS row_id_field_value FROM {} WHERE given_name ILIKE '{}' AND family_name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'ods.core_users'
query_14 = "SELECT 14 AS query_num, '{}' AS table, 'username' AS pii_field_01, username AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_dwh_id' AS row_id_field, person_dwh_id::varchar AS row_id_field_value FROM {} WHERE username ILIKE '{}'".format(str(table), table, email)

table = 'ods.mcloud_subscribers'
query_15 = "SELECT 15 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'subscribe_id' AS row_id_field, subscribe_id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)

table = 'ods.mongodb_de_crefo_attributes'
query_16 = "SELECT 16 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)

table = 'ods.mongodb_documents_info'
query_17 = "SELECT 17 AS query_num, '{}' AS table, 'name' AS pii_field_01, name AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'user_id' AS row_id_field, user_id::varchar AS row_id_field_value FROM {} WHERE name ILIKE '{}' AND name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'ods.schufa_raw_basic'
query_18 = "SELECT 18 AS query_num, '{}' AS table, 'vorname' AS pii_field_01, vorname AS pii_field_01_value, 'nachname' AS pii_field_02, nachname AS pii_field_02_value, 'loans_persons_role_dwh_id' AS row_id_field, loans_persons_role_dwh_id::varchar AS row_id_field_value FROM {} WHERE vorname ILIKE '{}' AND nachname ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'pre_staging.messages'
query_19 = "SELECT 19 AS query_num, '{}' AS table, 'receiver' AS pii_field_01, receiver AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE receiver ILIKE '{}'".format(str(table), table, email)

table = 'pre_staging.users'
query_20 = "SELECT 20 AS query_num, '{}' AS table, 'username' AS pii_field_01, username AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username ILIKE '{}'".format(str(table), table, email)
query_21 = "SELECT 21 AS query_num, '{}' AS table, 'username_canonical' AS pii_field_01, username_canonical AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username_canonical ILIKE '{}'".format(str(table), table, email)
query_22 = "SELECT 22 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)
query_23 = "SELECT 23 AS query_num, '{}' AS table, 'email_canonical' AS pii_field_01, email_canonical AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE email_canonical ILIKE '{}'".format(str(table), table, email)

table = 'reporting.cashflows'
query_24 = "SELECT 24 AS query_num, '{}' AS table, 'remittance_information' AS pii_field_01, remittance_information AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'user_dwh_id' AS row_id_field, user_dwh_id::varchar AS row_id_field_value FROM {} WHERE remittance_information ILIKE '{}' AND remittance_information ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'reporting.cashflows_v2'
query_25 = "SELECT 25 AS query_num, '{}' AS table, 'remittance_information' AS pii_field_01, remittance_information AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'user_id' AS row_id_field, user_id::varchar AS row_id_field_value FROM {} WHERE remittance_information ILIKE '{}' AND remittance_information ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'salesforce.contacts'
query_26 = "SELECT 26 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_dwh_id' AS row_id_field, person_dwh_id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)

table = 'salesforce.partners'
query_27 = """SELECT 27 AS query_num, '{}' AS table, 'Name' AS pii_field_01, "Name" AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'Id' AS row_id_field, "Id"::varchar AS row_id_field_value FROM {} WHERE "Name" ILIKE '{}' AND "Name" ILIKE '{}'""".format(str(table), table, first_name, last_name)
query_28 = """SELECT 28 AS query_num, '{}' AS table, 'Email__c' AS pii_field_01, "Email__c" AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'Id' AS row_id_field, "Id"::varchar AS row_id_field_value FROM {} WHERE "Email__c" ILIKE '{}'""".format(str(table), table, email)

table = 'st.general_information'
query_29 = "SELECT 29 AS query_num, '{}' AS table, 'warrantor_name_2' AS pii_field_01, warrantor_name_2 AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE warrantor_name_2 ILIKE '{}' AND warrantor_name_2 ILIKE '{}'".format(str(table), table, first_name, last_name)
query_30 = "SELECT 30 AS query_num, '{}' AS table, 'warrantor_name_3' AS pii_field_01, warrantor_name_3 AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE warrantor_name_3 ILIKE '{}' AND warrantor_name_3 ILIKE '{}'".format(str(table), table, first_name, last_name)
query_31 = "SELECT 31 AS query_num, '{}' AS table, 'warrantor_name_4' AS pii_field_01, warrantor_name_4 AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE warrantor_name_4 ILIKE '{}' AND warrantor_name_4 ILIKE '{}'".format(str(table), table, first_name, last_name)
query_32 = "SELECT 32 AS query_num, '{}' AS table, 'warrantor_name' AS pii_field_01, warrantor_name AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE warrantor_name ILIKE '{}' AND warrantor_name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'staging.core_de_addresses'
query_33 = "SELECT 33 AS query_num, '{}' AS table, 'name' AS pii_field_01, name AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE name ILIKE '{}' AND name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'staging.core_de_bank_accounts'
query_34 = "SELECT 34 AS query_num, '{}' AS table, 'account_holder' AS pii_field_01, account_holder AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'user_id' AS row_id_field, user_id::varchar AS row_id_field_value FROM {} WHERE account_holder ILIKE '{}' AND account_holder ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'staging.core_de_bank_responses'
query_35 = "SELECT 35 AS query_num, '{}' AS table, 'debtor_account_holder' AS pii_field_01, debtor_account_holder AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE debtor_account_holder ILIKE '{}' AND debtor_account_holder ILIKE '{}'".format(str(table), table, first_name, last_name)
query_36 = "SELECT 36 AS query_num, '{}' AS table, 'creditor_account_holder' AS pii_field_01, creditor_account_holder AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE creditor_account_holder ILIKE '{}' AND creditor_account_holder ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'staging.core_de_bank_transactions'
query_37 = "SELECT 37 AS query_num, '{}' AS table, 'creditor_name' AS pii_field_01, creditor_name AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE creditor_name ILIKE '{}' AND creditor_name ILIKE '{}'".format(str(table), table, first_name, last_name)
query_38 = "SELECT 38 AS query_num, '{}' AS table, 'remittance_information' AS pii_field_01, remittance_information AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE remittance_information ILIKE '{}' AND remittance_information ILIKE '{}'".format(str(table), table, first_name, last_name)
query_39 = "SELECT 39 AS query_num, '{}' AS table, 'debtor_name' AS pii_field_01, debtor_name AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE debtor_name ILIKE '{}' AND debtor_name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'staging.core_de_messages'
query_40 = "SELECT 40 AS query_num, '{}' AS table, 'receiver' AS pii_field_01, receiver AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE receiver ILIKE '{}'".format(str(table), table, email)
query_41 = "SELECT 41 AS query_num, '{}' AS table, 'sender' AS pii_field_01, sender AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE sender ILIKE '{}'".format(str(table), table, email)

table = 'staging.core_de_persons'
query_42 = "SELECT 42 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)
query_43 = "SELECT 43 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)
query_44 = "SELECT 44 AS query_num, '{}' AS table, 'given_name' AS pii_field_01, given_name AS pii_field_01_value, 'family_name' AS pii_field_02, family_name AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE given_name ILIKE '{}' AND family_name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'staging.core_de_referrals'
query_45 = "SELECT 45 AS query_num, '{}' AS table, 'referred_email' AS pii_field_01, referred_email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'referrer_id' AS row_id_field, referrer_id::varchar AS row_id_field_value FROM {} WHERE referred_email ILIKE '{}'".format(str(table), table, email)

table = 'staging.core_de_users'
query_46 = "SELECT 46 AS query_num, '{}' AS table, 'username' AS pii_field_01, username AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username ILIKE '{}'".format(str(table), table, email)
query_47 = "SELECT 47 AS query_num, '{}' AS table, 'username' AS pii_field_01, username AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username ILIKE '{}'".format(str(table), table, email)
query_48 = "SELECT 48 AS query_num, '{}' AS table, 'username_canonical' AS pii_field_01, username_canonical AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username_canonical ILIKE '{}'".format(str(table), table, email)
query_49 = "SELECT 49 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)
query_50 = "SELECT 50 AS query_num, '{}' AS table, 'email_canonical' AS pii_field_01, email_canonical AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE email_canonical ILIKE '{}'".format(str(table), table, email)

table = 'staging.core_de_users_audits'
query_51 = "SELECT 51 AS query_num, '{}' AS table, 'username' AS pii_field_01, username AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username ILIKE '{}'".format(str(table), table, email)
query_52 = "SELECT 52 AS query_num, '{}' AS table, 'username' AS pii_field_01, username AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username ILIKE '{}'".format(str(table), table, email)
query_53 = "SELECT 53 AS query_num, '{}' AS table, 'username_canonical' AS pii_field_01, username_canonical AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username_canonical ILIKE '{}'".format(str(table), table, email)
query_54 = "SELECT 54 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)
query_55 = "SELECT 55 AS query_num, '{}' AS table, 'email_canonical' AS pii_field_01, email_canonical AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE email_canonical ILIKE '{}'".format(str(table), table, email)

table = 'staging.core_nl_addresses'
query_56 = "SELECT 56 AS query_num, '{}' AS table, 'name' AS pii_field_01, name AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE name ILIKE '{}' AND name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'staging.core_nl_messages'
query_57 = "SELECT 57 AS query_num, '{}' AS table, 'receiver' AS pii_field_01, receiver AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE receiver ILIKE '{}'".format(str(table), table, email)
query_58 = "SELECT 58 AS query_num, '{}' AS table, 'sender' AS pii_field_01, sender AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE sender ILIKE '{}'".format(str(table), table, email)

table = 'staging.core_nl_persons'
query_59 = "SELECT 59 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)
query_60 = "SELECT 60 AS query_num, '{}' AS table, 'given_name' AS pii_field_01, given_name AS pii_field_01_vale, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'id' AS row_id_field, id::varchar AS row_id_field_value FROM {} WHERE given_name ILIKE '{}' AND family_name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'staging.core_nl_referrals'
query_61 = "SELECT 61 AS query_num, '{}' AS table, 'referred_email' AS pii_field_01, referred_email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'referrer_id' AS row_id_field, referrer_id::varchar AS row_id_field_value FROM {} WHERE referred_email ILIKE '{}'".format(str(table), table, email)
query_62 = "SELECT 62 AS query_num, '{}' AS table, 'referred_email' AS pii_field_01, referred_email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'referrer_id' AS row_id_field, referrer_id::varchar AS row_id_field_value FROM {} WHERE referred_email ILIKE '{}'".format(str(table), table, email)

table = 'staging.core_nl_users'
query_63 = "SELECT 63 AS query_num, '{}' AS table, 'username' AS pii_field_01, username AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username ILIKE '{}'".format(str(table), table, email)
query_64 = "SELECT 64 AS query_num, '{}' AS table, 'username_canonical' AS pii_field_01, username_canonical AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username_canonical ILIKE '{}'".format(str(table), table, email)
query_65 = "SELECT 65 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)
query_66 = "SELECT 66 AS query_num, '{}' AS table, 'email_canonical' AS pii_field_01, email_canonical AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE email_canonical ILIKE '{}'".format(str(table), table, email)

table = 'staging.core_nl_users_audits'
query_67 = "SELECT 67 AS query_num, '{}' AS table, 'username' AS pii_field_01, username AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username ILIKE '{}'".format(str(table), table, email)
query_68 = "SELECT 68 AS query_num, '{}' AS table, 'username_canonical' AS pii_field_01, username_canonical AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE username_canonical ILIKE '{}'".format(str(table), table, email)
query_69 = "SELECT 69 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)
query_70 = "SELECT 70 AS query_num, '{}' AS table, 'email_canonical' AS pii_field_01, email_canonical AS pii_field_01_value, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'person_id' AS row_id_field, person_id::varchar AS row_id_field_value FROM {} WHERE email_canonical ILIKE '{}'".format(str(table), table, email)

table = 'direct_mail.contacts'
query_71 = "SELECT 71 AS query_num, '{}' AS table, 'first_name' AS pii_field_01, first_name AS pii_field_01_vale, 'last_name' AS pii_field_02, last_name AS pii_field_02_value, 'contact_id' AS row_id_field, contact_id::varchar AS row_id_field_value FROM {} WHERE first_name ILIKE '{}' AND last_name ILIKE '{}'".format(str(table), table, first_name, last_name)
query_72 = "SELECT 72 AS query_num, '{}' AS table, 'email' AS pii_field_01, email AS pii_field_01_vale, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'contact_id' AS row_id_field, contact_id::varchar AS row_id_field_value FROM {} WHERE email ILIKE '{}'".format(str(table), table, email)

table = 'salesforce.investor_or_company_accounts'
query_73 = "SELECT 73 AS query_num, '{}' AS table, 'personemail' AS pii_field_01, personemail AS pii_field_01_vale, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'sf_id' AS row_id_field, sf_id::varchar AS row_id_field_value FROM {} WHERE personemail ILIKE '{}'".format(str(table), table, email)
query_74 = "SELECT 74 AS query_num, '{}' AS table, 'firstname' AS pii_field_01, firstname AS pii_field_01_vale, 'lastname' AS pii_field_02, lastname AS pii_field_02_value, 'sf_id' AS row_id_field, sf_id::varchar AS row_id_field_value FROM {} WHERE firstname ILIKE '{}' AND lastname ILIKE '{}'".format(str(table), table, first_name, last_name)
query_75 = "SELECT 75 AS query_num, '{}' AS table, 'name' AS pii_field_01, name AS pii_field_01_vale, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'sf_id' AS row_id_field, sf_id::varchar AS row_id_field_value FROM {} WHERE name ILIKE '{}' AND name ILIKE '{}'".format(str(table), table, first_name, last_name)

table = 'salesforce.loan_opportunities'
query_76 = "SELECT 76 AS query_num, '{}' AS table, 'name' AS pii_field_01, name AS pii_field_01_vale, 'NA' AS pii_field_02, 'NA' AS pii_field_02_value, 'sf_id' AS row_id_field, sf_id::varchar AS row_id_field_value FROM {} WHERE name ILIKE '{}' AND name ILIKE '{}'".format(str(table), table, first_name, last_name)

query_list = [query_01, query_02, query_03, query_04, query_05, query_06, query_07, query_08, query_09, query_10
              , query_11, query_12, query_13, query_14, query_15, query_16, query_17, query_18, query_19, query_20
              , query_21, query_22, query_23, query_24, query_25, query_26, query_27, query_28, query_29, query_30
              , query_31, query_32, query_33, query_34, query_35, query_36, query_37, query_38, query_39, query_40
              , query_41, query_42, query_43, query_44, query_45, query_46, query_47, query_48, query_49, query_50
              , query_51, query_52, query_53, query_54, query_55, query_56, query_57, query_58, query_59, query_60
              , query_61, query_62, query_63, query_64, query_65, query_66, query_67, query_68, query_69, query_70
              , query_71, query_72, query_73, query_74, query_75, query_76]

conn = connect_dwh()

# Execute
start = time.time()
query_num = 1
df_dict = {}
df_list = []
for query in query_list:
    print('Running Query: ', query_num)
    df = pd.read_sql(sql = query, con = conn)
    print('Query {} returned {} row(s)'.format(query_num, len(df)))
    df_list.append(df)
    df_dict.update({query_num:len(df)})
    query_num += 1

# close the communication with the PostgreSQL
conn.close()
print('db connection closed')

#print(df_dict)
for key in df_dict:
    if(df_dict[key]!=0):
        print(key, end="\t")
        print(df_dict[key])

# df = pd.concat(df_list, sort = False)
# df = df[['query_num', 'table', 'pii_field_01', 'pii_field_01_value', 'pii_field_02', 'pii_field_02_value', 'row_id_field', 'row_id_field_value']]
# df.to_csv('rtbf_{}_{}_{}.csv'.format(first_name, last_name, datetime.now().strftime('%Y-%m-%d')), sep = ',', index = False)
end = time.time()
print('file has been exported in {:.2f} seconds.'.format(end - start))
#quit()
