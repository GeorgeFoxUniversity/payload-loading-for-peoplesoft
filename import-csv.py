"""
Application - Load job payment items into the PS Staging table.

Usage:
    python import-csv.py example.csv 2019-01-01 2019-01-31 PAYGROUP_CODE COMPANY_CODE --config config.ini
"""

__author__ = 'Joel Kelley'
__copyright__ = "Copyright 2019, George Fox University"
__licensee__ = 'MIT'
__version__ = "0.01"
__email__ = "jkelley@georgefox.edu"

import os
from configparser import ConfigParser
from pprint import pprint as pp
from datetime import datetime
import sys
import argparse
import time
import re

import cx_Oracle
import pandas as pd
import dateparser

WAIT = 10



INSERT_SQL = """insert into sysadm.PS_PSHUP_TXN(
    pu_source, creation_dt, company, paygroup, pay_end_dt, off_cycle,
    earns_begin_dt, earns_end_dt, emplid, empl_rcd, seqno, pu_txn_type,
    pu_txn_status, page_num, line_num, addl_nbr, sepchk, deptid, jobcode,
    position_nbr, acct_cd, gl_pay_type, pu_distribute, erncd,
    addl_pay_shift, addl_seq, addlpay_reason, oth_hrs, hourly_rt,
    ok_to_pay, disable_dir_dep, state, locality, tax_periods, tax_method,
    ded_taken, ded_subset_id, ded_taken_genl, ded_subset_genl,
    business_unit, comp_ratecd, tax_class, one_time_cd, ctx_class,
    plan_type, benefit_plan, dedcd, ded_class, ded_calc, ded_rate_pct,
    pu_amt, manual_check, paycheck_nbr, ded_slstx_class, ex_doc_id,
    ex_doc_type, ex_line_nbr, currency_cd, override_only, grossup,
    vc_plan_id, vc_payout_prd_id, gb_group_id, applid, award_date, eim_key,
    payout_dt, garnid, garn_one_time_cd, garn_priority, garn_ded_calc,
    garn_ded_pct, garn_ded_amt, cmpny_fee_pct, cmpny_fee_amt,
    payee_fee_pct, payee_fee_amt, py_garn_exempt, check_dt,
    pu_txn_manual_chg, work_psd_cd, res_psd_cd, oprid) 
VALUES( 
    :pu_source, :creation_dt, :company, :paygroup, :pay_end_dt, :off_cycle,
    :earns_begin_dt, :earns_end_dt, :emplid, :empl_rcd, :seqno,
    :pu_txn_type, :pu_txn_status, :page_num, :line_num, :addl_nbr, :sepchk,
    :deptid, :jobcode, :position_nbr, :acct_cd, :gl_pay_type,
    :pu_distribute, :erncd, :addl_pay_shift, :addl_seq, :addlpay_reason,
    :oth_hrs, :hourly_rt, :ok_to_pay, :disable_dir_dep, :state, :locality,
    :tax_periods, :tax_method, :ded_taken, :ded_subset_id, :ded_taken_genl,
    :ded_subset_genl, :business_unit, :comp_ratecd, :tax_class,
    :one_time_cd, :ctx_class, :plan_type, :benefit_plan, :dedcd,
    :ded_class, :ded_calc, :ded_rate_pct, :pu_amt, :manual_check,
    :paycheck_nbr, :ded_slstx_class, :ex_doc_id, :ex_doc_type,
    :ex_line_nbr, :currency_cd, :override_only, :grossup, :vc_plan_id,
    :vc_payout_prd_id, :gb_group_id, :applid, :award_date, :eim_key,
    :payout_dt, :garnid, :garn_one_time_cd, :garn_priority, :garn_ded_calc,
    :garn_ded_pct, :garn_ded_amt, :cmpny_fee_pct, :cmpny_fee_amt,
    :payee_fee_pct, :payee_fee_amt, :py_garn_exempt, :check_dt,
    :pu_txn_manual_chg, :work_psd_cd, :res_psd_cd, :oprid
    )"""




def ps_insert(db, emplid, empl_rcd, seq_no, earning_code, earns_begin, earns_end, amount=0, hours=0, paygroup = 'EMP', disable_direct_deposit='N', acct_cd=' ', company='CMP'):
    """ Insert an earnings code row into sysadm.PS_PSHUP_TXN for use with "Load
        Paysheet Trasacations".  

        :param Connection db: An open database connection the PS HR database
        :param str emplid: A person's emplid 
        :param int empl_rcd: A Employee Record number, i.e. which employee instance number is this for.
        :parm int seq_no: A zero indexed sequence number, each combination of
                          emplid, empl_rcd, seq_no must be unique.  So if you
                          want to load more than one earnings code for
                          person/employee instance, you need to increment the
                          sequence number.  Do not skip a number.
        :param str earning_code: The earnings code to use, this must already 
                                 be setup in your PS HR instance 
        :param Date earns_end: The end of the PS paysheet earns period
        :param Date earns_begin: The start of the PS paysheet earns period
        :param float amount: The dollar amount for to be loaded for this earning 
                             code.  This cannot be used at the same time as hours.
        :param float hours: The number of hours to be loaded for this eaerning code.
                            This cannot be set at the same time as amount.
        :param str paygroup: The PeopleSoft paygroup these entries belong to.  
                             This must already be set up in PS and must agree
                             with the PeopleSoft pay sheet you are loading
                             records for.
        :param str disable_direct_deposit: Should direct depoist be disabled for 
                                           this entry.  Note, this will not
                                           enable direct depoist if it has not
                                           already been set. Valid values are
                                           'Y' and 'N'.
        :acct_cd str acct_cd: The combo code to use for the payline.  We don't
                              know what will happen if you trying to load
                              multiple earnings codes with different combo
                              codes.  The combo code must already be valid in
                              PeopleSoft.

        
        Other Notes:
        
        On _pu_source_  PS Requires us to set the source as a 2 char string
        starting with an O.  Here we use OT for other, but as long as it starts
        with the capital letter O PeopleSoft will try to load it.

        For Reference:
            - http://docs.oracle.com/cd/E79521_01/hcm92pbr11/eng/hcm/hpay/task_DataInputRequirementsforThird-PartyPaysheetData-3e3d81.html#ua57c9be8-6326-4b37-8dac-355df2d501fb
            - http://docs.oracle.com/cd/E79521_01/hcm92pbr11/eng/hcm/hpay/task_LoadingPaysheetTransactions-3e3aab.html?pli=ul_d209e281_hpay
    """

    success = None
    
    params = {
            'pu_source' : 'OT', 
            'creation_dt' : datetime.now(),
            'company' : company,
            'paygroup' : paygroup,
            'pay_end_dt' : earns_end, #None, #Tweek
            'off_cycle' : 'N', #Tweek
            'earns_begin_dt' : earns_begin, 
            'earns_end_dt' : earns_end, 
            'emplid' : emplid,
            'empl_rcd' : empl_rcd, 
            'seqno' : seq_no, 
            'pu_txn_type' : 'E',
            'pu_txn_status' : 'A',
            'page_num' : 0,
            'line_num' : 0,
            'addl_nbr' : 0,
            'sepchk' : 0,
            'deptid' : ' ',
            'jobcode' : ' ',
            'position_nbr' : ' ',
            'acct_cd' : acct_cd,
            'gl_pay_type' : ' ',
            'pu_distribute' : ' ',
            'erncd' : earning_code, 
            'addl_pay_shift' : 'J', #Tweek
            'addl_seq' : 0,
            'addlpay_reason' : ' ',
            'oth_hrs' : hours, 
            'hourly_rt' : 0,
            'ok_to_pay' : ' ',
            'disable_dir_dep' : disable_direct_deposit, #Tweek
            'state' : ' ',
            'locality' : ' ',
            'tax_periods' : 0,
            'tax_method' : ' ',
            'ded_taken' : ' ',
            'ded_subset_id' : ' ',
            'ded_taken_genl' : ' ',
            'ded_subset_genl' : ' ',
            'business_unit' : ' ',
            'comp_ratecd' : ' ',
            'tax_class' : ' ',
            'one_time_cd' : ' ',
            'ctx_class' : ' ',
            'plan_type' : ' ',
            'benefit_plan' : ' ',
            'dedcd' : ' ',
            'ded_class' : ' ',
            'ded_calc' : ' ',
            'ded_rate_pct' : 0,
            'pu_amt' : amount,
            'manual_check' : 'N', #Tweek
            'paycheck_nbr' : 0,
            'ded_slstx_class' : ' ',
            'ex_doc_id' : ' ',
            'ex_doc_type' :  ' ',
            'ex_line_nbr' : 0,
            'currency_cd' : ' ',
            'override_only' : 'Y', #Tweek
            #'grossup' : 'Y', #Tweek
            'grossup' : 'N', #Tweek
            'vc_plan_id' : ' ',
            'vc_payout_prd_id' : ' ',
            'gb_group_id' : ' ',
            'applid' : ' ',
            'award_date' : None,
            'eim_key' : ' ',
            'payout_dt' : None,
            'garnid' :  ' ',
            'garn_one_time_cd' : ' ',
            'garn_priority' : 0,
            'garn_ded_calc' : ' ',
            'garn_ded_pct' : 0,
            'garn_ded_amt' : 0,
            'cmpny_fee_pct' : 0,
            'cmpny_fee_amt' : 0,
            'payee_fee_pct' : 0,
            'payee_fee_amt' : 0,
            'py_garn_exempt' : ' ',
            'check_dt' : None,
            'pu_txn_manual_chg' : ' ',
            'work_psd_cd' : ' ',
            'res_psd_cd' : ' ',
            'oprid' : ' ',
            }
            
    
    cur = db.cursor()
    success = cur.execute(INSERT_SQL, params)
    cur.close()
    
    return cur.rowcount




def clean_ps_table(ps):

    c = ps.cursor()
    ps.begin()
    c.execute("delete from sysadm.PS_PSHUP_TXN where pu_source = 'OT'")
    ps.commit()
    print("\nRemoved {count} rows from PS_PSHUP_TXN".format(count=c.rowcount))


def get_db_from_env(prefix='', sysdba=False):
    
    host = os.environ.get(prefix+'DATABASE_HOST')
    user = os.environ.get(prefix+'DATABASE_USER')
    password = os.environ.get(prefix+'DATABASE_PASSWORD')
    sid = os.environ.get(prefix+'DATABASE_SID')
    service_name = os.environ.get(prefix+'DATABASE_SERVICE_NAME')
    port = os.environ.get(prefix+'DATABASE_PORT')
    
    if service_name:
        dsnstr = cx_Oracle.makedsn( host, port, service_name=service_name)
    else:
        dsnstr = cx_Oracle.makedsn( host, port, sid)

    if sysdba:
        db = cx_Oracle.connect(user=user, password=password, dsn=dsnstr, mode=cx_Oracle.SYSDBA)
    else:
        db = cx_Oracle.connect(user=user, password=password, dsn=dsnstr)

    return db


def get_db_from_config(config, sysdba=False):
    
     
    host = config['connection'].get('host') 
    user = config['connection'].get('user') 
    password = config['connection'].get('password')
    sid = config['connection'].get('sid')
    service_name = config['connection'].get('service_name')
    port = config['connection'].get('port')
    
    if service_name:
        dsnstr = cx_Oracle.makedsn( host, port, service_name=service_name)
    else:
        dsnstr = cx_Oracle.makedsn( host, port, sid)

    if sysdba:
        db = cx_Oracle.connect(user=user, password=password, dsn=dsnstr, mode=cx_Oracle.SYSDBA)
    else:
        db = cx_Oracle.connect(user=user, password=password, dsn=dsnstr)

    return db

    pass

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('csv', type=str, help="CSV file to import")
    parser.add_argument('pay_start', type=str, help="Pay Start date")
    parser.add_argument('pay_end', type=str, help="Pay End date")
    parser.add_argument('pay_group', type=str, help="Pay Group")  
    parser.add_argument('company', type=str, help="Company Code")  
    parser.add_argument('--config', type=str, help="Path to Config File")
    parser.add_argument('--as-sysdba', help="Access the PeopleSoft Database as SYSDBA (you don't need this unless you know why it's here)", action = 'store_true')
    args = parser.parse_args()
    
    if args.config:
        config = ConfigParser()
        config.read(args.config)
        
        ps = get_db_from_config(config, sysdba = args.as_sysdba)
    else:
        ps = get_db_from_env('PS_', sysdba = args.as_sysdba)

    company = args.company
    pay_group = args.pay_group
    earns_begin = dateparser.parse(args.pay_start)
    earns_end = dateparser.parse(args.pay_end)
    
    # Read in CSV
    df = pd.read_csv(args.csv, dtype={'combo_code':str, 'earning_code':str}) 
    
    # Convert the Total column to a regular number.
    df['amount'] = df.Total.apply(lambda x: float(re.sub(r'[^\d.]', '', x)) if isinstance(x, str) else x)

    # Only include rows with a non null emplid
    df = df[ ~pd.isnull(df.emplid) ] 
    
    
    message = "In {wait} seconds the old PS_PSHUP_TXN records " \
              "with a source of 'OT' will be deleted.  Then " \
              "all sane records for {filename} will be inserted " \
              "with an earns_begin date of {earns_begin} and an " \
              "earns_end date of {earns_end} for pay group {pay_group} in {company}.".format(wait=WAIT, 
                                                     filename=args.csv, 
                                                     pay_group=pay_group, 
                                                     company=company, 
                                                     earns_begin=earns_begin.strftime('%Y-%m-%d'), 
                                                     earns_end=earns_end.strftime('%Y-%m-%d'))
    print("\nWarning:")
    print(message)
    time.sleep(WAIT)
    
    clean_ps_table(ps)
    
    def insert(row):
        """
        For each row in the dataframe insert a row in PS_PSHUP_TXN
        """
        disable_direct_deposit = row.disable_direct_deposit if ('disable_direct_deposit' in df.columns and row.disable_direct_deposit) else None 
        emplid = row.emplid
        empl_rcd = row.empl_rcd if ('empl_rcd' in df.columns and row.empl_rcd) else 0 
        seq_no = row.seq_no if ('seq_no' in df.columns and row.seq_no) else 0 
        earning_code = row.earning_code # must match a ps value
        hours = row.hours if ('hours' in df.columns and row.hours) else 0 
        amount = row.amount if ('amount' in df.columns and row.amount) else 0 
        acct_cd= row.combo_code if ('combo_code' in df.columns and row.combo_code) else ' ' 
        
        if disable_direct_deposit:
            ps_insert(ps, emplid, empl_rcd, seq_no, earning_code, earns_begin, earns_end, amount=amount, hours=hours, paygroup=pay_group, disable_direct_deposit=disable_direct_deposit, acct_cd=acct_cd, company=company)
        else:
            ps_insert(ps, emplid, empl_rcd, seq_no, earning_code, earns_begin, earns_end, amount=amount, hours=hours, paygroup=pay_group, acct_cd=acct_cd, company=company)

    
    # Insert each row
    df.apply(insert, axis=1)

    # Commit everything to the database
    ps.commit()
