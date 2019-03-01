# Payload Loading For Peoplesoft

Load job payment items into the PS Staging table.

## Setup using a virtual enviroment

 * Make the dependencies for the pip package **cx_Oracle** are already installed
 * Create the virtual enviroment `python -m venv venv`
 * Activate the virtual enviroment `source venv/bin/activate`
 * Install the pip packages: `pip install -r requirements.txt`

### If using a config file

* Create a config file: `cp config.ini.dist config.ini`
* Add your database settings to `config.ini`

## Running

### Command Line Arguments

* **csv:** Path to the CSV file to load
* **pay_start:** Earns begin date, this needs to match the current PS Paysheets entry for the target pay group.
* **pay_end:** Earns end date, this needs to match the current PS Paysheets entry for the target pay group.
* **pay_group:** The PeopleSoft pay group these records are for.
* **--config:** Path of a config containing db connection details.  If this isn't given it will check your environmental variables for connection details. 
* **--as-sysdba:** Connect to the database as sysdba.  Probably not something you want to do in production.


### Environmental Variables

If a config file isn't passed in, the script will look at the following
environmental variables for the database connection details.

* `PS_DATABASE_HOST`
* `PS_DATABASE_USER`
* `PS_DATABASE_PASSWORD`
* `PS_DATABASE_SID`
* `PS_DATABASE_PORT`


### Valid Column Headers

* `disable_direct_deposit` - Disable Direct Deposit for this entry.
* `EMPLID`- PS Emplid of job for this entry.
* `earning_code`- Earning code for this entry.
* `hours`- Hours to be loaded for this entry. Mutually exclusive with amount.
* `amount` - Amount (money) to be loaded for this entry. Mutually exclusive with hours.
* `combo_code`- Match the payment to an account string.


### Example

```
python import-csv.py example.csv 2019-01-01 2019-01-31 PAYGROUP_CODE --config config.ini
```


## Copyright Stuff

* Written by: Joel Kelley <jkelley@georgefox.edu>
* Copyright: 2019, George Fox University
