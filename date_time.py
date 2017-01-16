#!/usr/bin/env python

from datetime import datetime
from business_calendar import Calendar, MO, TU, WE, TH, FR
import sys, os, json, functools, holidays, glob
from dateutil.parser import *
from dateutil.relativedelta import relativedelta
import argparse

home = os.getenv("HOME")
need_format = "%Y-%m-%d"

def define_headers(headers):
    index = 0
    header_set = {}
    headers = headers.split(';')
    for header in headers:
        header_set[header] = index
        index = index + 1
    return header_set

def bucket(data_rows, column_num):
    buckets = {}
    for row in data_rows:
        k = row[column_num]
        if k in buckets.keys():
            buckets[k].append(row)
        else:
            buckets[k] = []
            buckets[k].append(row)
    return buckets

def filter_mandatory(data_rows, mandatory_filters, header_lookup):
    filtered_buckets = {}
    if ('by_bank' in mandatory_filters.keys()):
        filtered_buckets = bucket(data_rows, header_lookup[mandatory_filters['by_bank']])
    return filtered_buckets

def filter_by_bucket(data_rows, bucket_filters, header_lookup):
    for column_name, column_values in bucket_filters.items():
        if (column_name in header_lookup.keys()):
            for bank, bank_rows in data_rows.items():
                filtered_rows = filter(lambda row: (row[header_lookup[column_name]] in column_values), bank_rows)
                data_rows[bank] = filtered_rows
        else:
            print "Bucket " + column_name + " not a valid filter"
            continue
    return data_rows

def define_calendar(holiday_dates):
    return Calendar(workdays=[MO,TU,WE,TH,FR], holidays=holiday_dates)

def define_target_dates(calendar, start, end):
    start = datetime.date(datetime.strptime(start, need_format))
    end = datetime.date(datetime.strptime(end, need_format))
    date_range = calendar.range(start, end)
    date_range_str = map(lambda date: (datetime.strftime(date, need_format)), date_range)
    return date_range_str

def filter_date_row(bank_row, start_date, end_date, cal, header_lookup):
    trade_date = datetime.strptime(bank_row[header_lookup['TradeDate']], need_format)
    settlement_date = datetime.strptime(bank_row[header_lookup['SettlementDate']], need_format)
    return ((trade_date >= start_date and trade_date < end_date) and
            (cal.isbusday(datetime.date(trade_date))) and
            (settlement_date in [trade_date, cal.addbusdays(datetime.date(trade_date), 1), cal.addbusdays(datetime.date(trade_date), 2)]))

def filter_by_dates(data_rows, calendar, date_filters, header_lookup):
    if date_filters:
        for bank, bank_rows in data_rows.items():
            start_date = datetime.strptime(date_filters['startDate'], need_format)
            end_date = datetime.strptime(date_filters['endDate'], need_format)
            filtered_rows = [bank_row for bank_row in bank_rows if filter_date_row(bank_row, start_date, end_date, calendar, header_lookup) == True]
            data_rows[bank] = filtered_rows
    return data_rows

def default_tenors():
    return {
            "One Week" : [{'days':7}, "1"],
            "Two Week" : [{'days':14}, "2"],
            "One Month" : [{'months':1}, "5"],
            "Three Month" : [{'month':3}, "5"],
            "Six Month" : [{'month':6}, "10"],
            "Twelve Month" : [{'months':12}, "10"]
    }

def find_tenor(bank_rows, corridor_filters, cal, header_lookup):
    tenors = {}
    total_rate = 0
    for bank_row in bank_rows:
        settlement_date = datetime.strptime(bank_row[header_lookup['SettlementDate']], need_format)
        true_maturity_date = datetime.date(datetime.strptime(bank_row[header_lookup['MaturityDate']], need_format))
        trade_date = datetime.strptime(bank_row[header_lookup['TradeDate']], need_format)
        trade_date_str = datetime.strftime(trade_date, need_format)
        upper_trade_date = cal.addbusdays(datetime.date(trade_date), 2)
        volume = float(bank_row[header_lookup['Transaction Nominal Amount']])
        rate = float(bank_row[header_lookup['DealRate']])
        for tenor_label, tenor_info in default_tenors().items():
            if trade_date_str not in tenors.keys():
                tenors[trade_date_str] = {}
            if tenor_label not in tenors[trade_date_str].keys():
                tenors[trade_date_str][tenor_label] = {}
            maturity_delta = tenor_info[0]
            corridor = corridor_filters[tenor_label] if (corridor_filters and tenor_label in corridor_filters) else int(tenor_info[1])
            neg_corridor = -1 * corridor
            delta_date = trade_date + relativedelta(**maturity_delta)
            lower_maturity_bound = cal.addbusdays(cal.adjust(datetime.date(delta_date), 1), neg_corridor)
            upper_delta_date = upper_trade_date + relativedelta(**maturity_delta)
            upper_maturity_bound = cal.addbusdays(cal.adjust(datetime.date(delta_date), 1), corridor)
            if (true_maturity_date >= lower_maturity_bound and true_maturity_date <= upper_maturity_bound):
                results = tenors[trade_date_str][tenor_label]
                if 'data' not in results.keys():
                    results['data'] = []
                    results['num_transactions'] = 0
                    results['sum_volumes'] = 0
                    results['vwar'] = 0
                results['data'].append(bank_row)
                total_rate = total_rate + (volume * rate)
                results['sum_volumes'] = results['sum_volumes'] + volume
                results['num_transactions'] = results['num_transactions'] + 1
                results['vwar'] = total_rate / results['sum_volumes']
    return tenors

def calculate_tenors(data_rows, calendar, corridor_filters, header_lookup):
    for bank, bank_rows in data_rows.items():
        rows_with_tenors = find_tenor(bank_rows, corridor_filters, calendar, header_lookup)
        data_rows[bank] = rows_with_tenors
    return data_rows

def find_date_range(cal, data_rows, header_lookup):
    earliest = datetime.strptime("2016-01-01", need_format)
    latest = earliest
    for bank, date_hash in data_rows.items():
        for date in date_hash.keys():
            date_comp = datetime.strptime(date, need_format)
            if (date_comp < earliest):
                earliest = date_comp
            if (date_comp > latest):
                latest = date_comp
    earliest = datetime.strftime(earliest, need_format)
    latest = datetime.strftime(latest, need_format)
    return define_target_dates(cal, earliest, latest)

def complete_tenors(data_rows, cal, period_filter, header_lookup):
    if period_filter:
        target_range = define_target_dates(cal, period_filter['startDate'], period_filter['endDate'])
    else:
        target_range = find_date_range(cal, data_rows, header_lookup)
    tenor_labels = default_tenors().keys()

    for bank, date_hash in data_rows.items():
        empty_hash = {}
        empty_dates = list(set(target_range) - set(date_hash.keys()))
        for empty_date in empty_dates:
            results = {}
            for tenor_label in tenor_labels:
                results[tenor_label] = {}
                results[tenor_label]['data'] = []
                results[tenor_label]['num_transactions'] = 0
                results[tenor_label]['sum_volumes'] = 0
                results[tenor_label]['vwar'] = "X"
            empty_hash[empty_date] = results
        print empty_hash
        date_hash.update(empty_hash)
        data_rows[bank] = date_hash
    return data_rows, target_range


def print_data_by_tenor(data_rows, target_days):
    tenor_labels = default_tenors().keys()
    tables = {}
    for tenor in tenor_labels:
        tables[tenor] = {}
        table_for_tenor = []
        banks = "   |".join(data_rows.keys())
        banks = "   |" + banks
        rate_vol = "    |"
        for i in range(0, len(data_rows.keys())):
            rate_vol = rate_vol + "rate  |vol  |"
        table_for_tenor.append(banks)
        table_for_tenor.append(rate_vol)
        for day in target_days:
            day_row = [day]
            for bank, bank_data in data_rows.items():
                print day, tenor
                bank_day_data = bank_data[day][tenor]
                print bank_day_data
                day_row.append(bank_day_data['vwar'])
                day_row.append('|')
                day_row.append(bank_day_data['sum_volumes'])
            day_row = '   '.join(day_row)
            table_for_tenor.append(day_row)
        tables[tenor] = table_for_tenor
    return tables

def process_file(args):
    file_directory = args['file_directory']
    if (os.path.exists(file_directory)):
        csv_files = glob.glob(file_directory + "/*.csv")
        first = True
        new_csv_file = file_directory + "/merged.csv"
        new_fh = open(new_csv_file, 'w')
        new_rows = []
        return_rows = []
        for csv_file in csv_files:
            fh = open(csv_file, 'rb')
            header_row = fh.readline()
            if first:
                new_rows.append(header_row)
                header_set = define_headers(header_row)
            for row in fh:
                data = row.split(';')
                new_data = []
                for item in data:
                    try:
                        float(item)
                        new_data.append(item)
                    except ValueError:
                        try:
                            if (item != '' and item != '\n'):
                                time = parse(item)
                                new_time = time.strftime(need_format)
                                new_data.append(new_time)
                            else:
                                if (item == ''):
                                    new_data.append(item)
                        except ValueError:
                            new_data.append(item)
                return_rows.append(new_data)
                new_row = ';'.join(new_data)
                new_rows.append(new_row)
                first = False
        rows_string = '\n'.join(new_rows)
        new_fh.write(rows_string)
        new_fh.close()
        return return_rows, header_set
    else:
        print "Directory does not exist"

def run(args):
    data_rows, header_set = process_file(args)
    holiday_dates = sorted(holidays.ECB(years=[2016, 2017, 2018, 2019]).keys())
    calendar = define_calendar(holiday_dates)
    filters = args['filters']
    if 'mandatory' in filters.keys():
        data_rows = filter_mandatory(data_rows, filters['mandatory'], header_set)
    if 'bucket' in filters.keys():
        data_rows = filter_by_bucket(data_rows, filters['bucket'], header_set)
    if 'period' in filters.keys():
        data_rows = filter_by_dates(data_rows, calendar, filters['period'], header_set)
        if 'corridor' in filters.keys():
            data_rows = calculate_tenors(data_rows, calendar, filters['corridor'], header_set)
        else:
            data_rows = calculate_tenors(data_rows, calendar, None, header_set)
        data_rows, target_days = complete_tenors(data_rows, calendar, filters['period'], header_set)
    else:
        data_rows = filter_by_dates(data_rows, calendar, None, header_set)
        if 'corridor' in filters.keys():
            data_rows = calculate_tenors(data_rows, calendar, filters['corridor'], header_set)
        else:
            data_rows = calculate_tenors(data_rows, calendar, None, header_set)
        data_rows, target_days = complete_tenors(data_rows, calendar, None, header_set)
    print print_data_by_tenor(data_rows, target_days)

parser = argparse.ArgumentParser()
parser.add_argument('--file_directory', required=True, dest='file_directory', type=str, help="File path to analyze")
parser.add_argument('--filters', type=json.loads, dest='filters', nargs='+', help="Filters you want to apply to current data set")
parser.add_argument('--filter_path', type=str, dest='filter_path', help='Path to file that holds json syntax for filtering')
args = vars(parser.parse_args())
if (args['filter_path']):
    if (os.path.exists(args['filter_path'])):
        filter_h = open(args['filter_path'], 'rb')
        filter_dict = json.load(filter_h)
        args['filters'] = filter_dict
run(args)

