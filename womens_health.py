import os
import csv
import sqlite3
import re
import requests
import xml.etree.ElementTree as ET
import datetime
import csv


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def validate_file():
    print("Validating records through USPS API...this may take some time.  Coffee break?")
    # url = 'https://secure.shippingapis.com/ShippingAPI.dll?API=CityStateLookup&XML={0}'
    usps_userid = '813BINDE5230'
    url = ('https://secure.shippingapis.com/ShippingApi.dll?API'
           '=Verify&XML=<AddressValidateRequest USERID="{userid}">'
           '<Revision>1</Revision><Address ID="0"><Address1>{addr1}</Address1>'
           '<Address2>{addr2}</Address2><City>{city}</City>'
           '<State>{state}</State><Zip5>{zip5}</Zip5><Zip4>{zip4}</Zip4>'
           '</Address></AddressValidateRequest>')

    db = sqlite3.connect('health.db')
    db.row_factory = dict_factory
    cursor = db.cursor()

    # sql = ("SELECT rowid, addr1, addr2, city, state, zip5, zip4 FROM health "
    #        "WHERE addr2 != '' LIMIT 1;")

    # sql = ("SELECT rowid, addr1, addr2, city, state, zip5, zip4 FROM health LIMIT 500;")

    sql = ("SELECT rowid, addr1, addr2, city, state, zip5, zip4 FROM health "
           "where upper(addr2) LIKE 'STE%' OR upper(addr2) LIKE 'SUITE%' "
           "OR upper(addr2) LIKE 'APT%' OR upper(addr2) LIKE 'APARTMENT%';")

    cursor.execute(sql)

    for rec in cursor.fetchall():
        # print(rec)
        # print(url.format(userid=usps_userid, **rec))

        if int(rec['rowid']) % 100 == 0:
            db.commit()
            print("Record {0}".format(rec['rowid']))

        response = requests.get(url.format(userid=usps_userid, **rec))
        tree = ET.fromstring(response.content)

        request_d = dict()
        for branch in tree:
            response_d = dict()
            for child in branch:
                response_d[child.tag] = child.text
            # print(response_d)

        if "Error" not in response_d:
            if "Address2" in response_d:
                sql = "UPDATE health SET cass_addr1 = '{0}' WHERE rowid = '{1}';".format(response_d['Address2'],
                                                                                         rec['rowid'])
                cursor.execute(sql)

            if "Address1" in response_d:
                sql = ("UPDATE health SET cass_addr2 = '{0}' "
                       "WHERE rowid = '{1}';".format(response_d['Address1'], rec['rowid']))
                cursor.execute(sql)

            if "City" in response_d:
                sql = "UPDATE health SET cass_city = '{0}' WHERE rowid = '{1}';".format(response_d['City'],
                                                                                        rec['rowid'])
                cursor.execute(sql)

            if "State" in response_d:
                sql = "UPDATE health SET cass_state = '{0}' WHERE rowid = '{1}';".format(response_d['State'],
                                                                                         rec['rowid'])
                cursor.execute(sql)

            if "Zip5" in response_d:
                zip9 = lambda x, y: "{0}-{1}".format(x, y) if y != "" else "{0}".format(x)
                sql = ("UPDATE health SET cass_Zip9 = '{0}' "
                       "WHERE rowid = '{1}';".format(zip9(response_d['Zip5'], response_d['Zip4']),
                                                    rec['rowid']))
                cursor.execute(sql)

            if "DeliveryPoint" in response_d:
                sql = "UPDATE health SET dp = '{0}' WHERE rowid = '{1}';".format(response_d['DeliveryPoint'],
                                                                                 rec['rowid'])
                cursor.execute(sql)

            if "CarrierRoute" in response_d:
                sql = "UPDATE health SET crrt = '{0}' WHERE rowid = '{1}';".format(response_d['CarrierRoute'],
                                                                                   rec['rowid'])
                cursor.execute(sql)

            if "DPVConfirmation" in response_d:
                sql = "UPDATE health SET dpv = '{0}' WHERE rowid = '{1}';".format(response_d['DPVConfirmation'],
                                                                                  rec['rowid'])
                cursor.execute(sql)

    db.commit()
    db.close()
    print("Validate complete")


def import_file():
    print("Importing file to SQLite database")
    srch = re.compile("Womens_Health[(0-9)]{4}\.txt")
    fle = [f for f in os.listdir(os.path.curdir) if srch.findall(f)]

    db = sqlite3.connect('health.db')
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS health;")
    db.commit()

    cursor.execute("CREATE TABLE health ("
                   "firstnam varchar(19), mi varchar(1), lstnam varchar(24), "
                   "addr1 varchar(34), addr2 varchar(33), city varchar(24), "
                   "state varchar(2), zip5 varchar(5), zip4 varchar(4), "
                   "group_cat varchar(20), product varchar(11), "
                   "flag varchar(20), cass_addr1 varchar(50), "
                   "cass_addr2 varchar(50), cass_city varchar(35), "
                   "cass_state varchar(2), cass_Zip9 varchar(15), "
                   "dp varchar(2), crrt varchar(4), dpv varchar(1));")

    with open(fle[0], 'r') as f:
        # throw out the first 10 lines
        for i in range(9):
            next(f)
        for n, line in enumerate(f):
            line = line.strip()

            firstnam = line[0:19].strip()
            mi = line[19:20].strip()
            lstnam = line[25:49].strip()
            addr1 = line[49:83].strip()
            addr2 = line[83:116].strip()
            city = line[116:140].strip()
            state = line[140:142].strip()
            zip5 = line[148:153].strip()
            zip4 = line[157:161].strip()
            group_cat = line[165:185].strip()
            product = line[185:196].strip()
            flag = line[196:198].strip()

            vals = (firstnam, mi, lstnam, addr1, addr2, city, state, zip5, zip4, group_cat, product, flag)


            sql = ("INSERT INTO health (firstnam, mI, lstnam, "
                   "addr1, addr2, city, state, zip5, zip4, "
                   "group_cat, product, flag) VALUES (?, "
                   "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")

            cursor.execute(sql, vals)

    db.commit()
    db.close()
    print("Import complete")


def export_file():
    print("Exporting revised file from SQLite database")
    db = sqlite3.connect('health.db')
    db.row_factory = dict_factory
    cursor = db.cursor()

    # sql = ("SELECT * FROM health;")
    sql = ("SELECT * FROM health where cass_addr1 IS NOT NULL;")
    cursor.execute(sql)

    dt = datetime.datetime.strftime(datetime.date.today(), "%Y%m%d")

    with open('Womens_Health_Reformat_{0}.txt'.format(dt), 'w', newline='') as s:
        headers = [description[0] for description in cursor.description]
        csvw = csv.DictWriter(s, headers, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        csvw.writeheader()
        for rec in cursor.fetchall():
            csvw.writerow(rec)

    db.close()
    print("Export complete")


def main():
    # TODO add updated field
    # TODO only import records to SQLite with the correct record len
    # TODO program PO Box swap
    # TODO replace '#' in apartment or suite fields
    # TODO program Apt, Ste combine
    """
        select *
        from health
        where upper(addr2) LIKE 'STE%'
            OR upper(addr2) LIKE 'SUITE%'
            OR upper(addr2) LIKE 'APT%'
            OR upper(addr2) LIKE 'APARTMENT%'
        ;
    """

    import_file()
    validate_file()
    export_file()


if __name__ == '__main__':
    main()