import os
import csv
import requests
import psycopg2

from datetime import date

totals_by_county = {}
def add_to_county(county, cases, tests):
	global totals_by_county
	
	if cases is None:
		cases = 0

	if county in totals_by_county:
		totals_by_county[county]['cases'] += cases
		totals_by_county[county]['tests'] += tests
	else:
		totals_by_county[county] = {'cases': cases, 'tests': tests}

today = date.today()
current_year = today.strftime('%Y')
current_month = today.strftime('%m')
current_date = today.strftime('%Y%m%d')

location_url_base = 'https://www.vdh.virginia.gov/content/uploads/sites/182'
input_filename = 'VDH-COVID-19-PublicUseDataset-ZIPCode.csv'

location_url = '%s/%s/%s/%s' % (location_url_base, current_year, current_month, input_filename)
data_by_zipcode_filename = 'data/by_zipcode/data_by_zipcode_%s.csv' % current_date
data_by_county_filename = 'data/by_county/data_by_county_%s.csv' % current_date

# load all zipcode-to-county mappings into a dictionary
zip_code_dict = {}
with open('data/zip_codes_by_county.csv') as zipcodes:
	csv_reader = csv.reader(zipcodes)
	header = next(csv_reader)
	for line in csv_reader:
		zip_code_dict[str(line[0])] = line[1]

# Get the input data
response = requests.get(location_url, verify=False)

# Iterate through the input data and clean up
# Also add the county name to each row

with open(data_by_zipcode_filename, 'w') as f:
	writer = csv.writer(f, quotechar="'")
	for idx, line in enumerate(response.iter_lines()):
		
		if idx < 1:
			continue
		
		line_pieces = line.decode('utf-8').split(',')

		if len(line_pieces) < 3:
			continue

		reported_dt = line_pieces[0]
		zip_code = line_pieces[1]

		if line_pieces[2] == 'Suppressed*':
			total_cases = None
		else:
			total_cases = int(line_pieces[2])
		
		if str(zip_code) in zip_code_dict:
			county = zip_code_dict[zip_code]

			if len(line_pieces) > 4:
				total_tests = int(line_pieces[3].strip('"') + line_pieces[4].strip('"'))
			else:
				total_tests = int(line_pieces[3])

			writer.writerow([zip_code, county, total_cases, total_tests])

			add_to_county(county, total_cases, total_tests)

with open(data_by_county_filename, 'w') as county_data:
	writer = csv.writer(county_data)
	for county in totals_by_county:
		writer.writerow([county, 
			totals_by_county[county]['cases'], 
			totals_by_county[county]['tests']])

try:

	novacovid_db_user = os.environ['NOVACOVID_DB_USER']
	novacovid_db_password = os.environ['NOVACOVID_DB_PASSWORD']
	novacovid_db_host = os.environ['NOVACOVID_DB_HOST']
	novacovid_db_port = os.environ['NOVACOVID_DB_PORT']

	connection = psycopg2.connect(
		user=novacovid_db_user,
		password=novacovid_db_password,
		host=novacovid_db_host,
		port=novacovid_db_port,
		database = novacovid_db_user)

	cursor = connection.cursor()
	cursor.execute('DELETE FROM covid_by_county WHERE report_dt = \'%s\'' % current_date)
	for county in totals_by_county:
		cursor.execute('INSERT INTO covid_by_county VALUES (\'%s\', \'%s\', %d, %d)' % (
			current_date,
			county,
			totals_by_county[county]['cases'],
			totals_by_county[county]['tests']
		))
	connection.commit()
except (Exception, psycopg2.Error) as error :
	print ("Error while connecting to PostgreSQL", error)
finally:
	if (connection):
		cursor.close()
		connection.close()