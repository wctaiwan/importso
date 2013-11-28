import sys
import time
import urllib2
import StringIO
import gzip
import json
import sqlite3


DB_PATH = 'db.so'
BASE_URL = 'http://api.stackexchange.com/2.1/'
COMMON_PARAM = 'site=stackoverflow&pagesize=100&order=asc&sort=creation'
QUESTION_FILTER = '!-MBjrRpSPf)KF1Y88PMbnL*CJER13vdyX'
ANS_LIST_FILTER = '!SrhSpQbKcdOREYx6dp'
ANS_CONTENT_FILTER = '!2.5jc5ezSU8kx1f7Sbwm('
DAY_SECS = 86399


def get_json(url):
	response = urllib2.urlopen(url)
	buf = StringIO.StringIO(response.read())
	f = gzip.GzipFile(fileobj=buf)
	return json.load(f)


def get_timestamp(date_str):
	return int(time.mktime(time.strptime(date_str, '%Y/%m/%d')))


def construct_question_url(start_date, end_date, page_num):
	return '{0}search?{1}&tagged=java;javascript&fromdate={2}&todate={3}&filter={4}&page={5}'.format(
		BASE_URL,
		COMMON_PARAM,
		start_date,
		end_date,
		QUESTION_FILTER,
		str(page_num))


def construct_ans_list_url(start_date, end_date, page_num):
	return '{0}answers?{1}&fromdate={2}&todate={3}&filter={4}&page={5}'.format(BASE_URL,
		COMMON_PARAM,
		start_date,
		end_date,
		ANS_LIST_FILTER,
		str(page_num))


def construct_ans_content_url(ids):
	ids_cast = [str(i) for i in ids]
	id_str = ';'.join(ids_cast)
	return '{0}answers/{1}?{2}&filter={3}'.format(BASE_URL, id_str, COMMON_PARAM, ANS_CONTENT_FILTER)


def print_error(url, data):
	print 'Received error; stopping'
	print 'URL: ' + url
	print str(data['error_id']) + ' ' + data['error_name']
	print data['error_message']


def get_questions(start_date, end_date, db):
	print 'Reading questions in date range'

	cursor = db.cursor()
	cursor.execute('SELECT question_id FROM question ORDER BY question_id DESC LIMIT 1')
	res = cursor.fetchone()
	if res is None:
		min_id = 0
	else:
		min_id = res[0]

	page_num = 1
	keep_running = True
	while keep_running:
		print 'Processing page ' + str(page_num)
		url = construct_question_url(start_date, end_date, page_num)
		data = get_json(url)

		if 'error_id' in data:
			print_error(url, data)
			return -1

		items = data['items']

		for row in items:
			try:
				if row['question_id'] <= min_id:
					continue
				tags = ', '.join(row['tags'])

				values = (row['question_id'],
					row['creation_date'],
					row['title'],
					row['body'],
					tags,
					row['score'],
					row.get('accepted_answer_id')) #None/NULL if not exists

				cursor.execute('INSERT INTO question VALUES (?, ?, ?, ?, ?, ?, ?)', values)
			except KeyError:
				print 'Received KeyError attemtping to process question ' + str(row.get('question_id'))

		if data['has_more'] is False:
			print 'Finished reading questions'
			keep_running = False
		elif data['quota_remaining'] == 0:
			print 'Quota exhausted; stopping'
			return -1
		else:
			time.sleep(0.5)
			if 'backoff' in data:
				print 'Backoff: ' + data['backoff'] + 's'
				time.sleep(data['backoff'])
			page_num += 1
			keep_running = True

	return 0


def get_answers(start_date, end_date, db):
	print 'Reading answers in date range'

	cursor = db.cursor()
	cursor.execute('SELECT answer_id FROM answer ORDER BY answer_id DESC LIMIT 1')
	res = cursor.fetchone()
	if res is None:
		min_id = 0
	else:
		min_id = res[0]

	print 'Obtaining list of answers with desired tags'
	ans_list = []

	page_num = 1
	keep_running = True
	while keep_running:
		print 'Processing page ' + str(page_num)
		url = construct_ans_list_url(start_date, end_date, page_num)
		data = get_json(url)

		if 'error_id' in data:
			print_error(url, data)
			return -1

		items = data['items']

		for row in items:
			try:
				if row['answer_id'] <= min_id:
					continue
				if 'java' in row['tags'] or 'javascript' in row['tags']:
					ans_list.append(row['answer_id'])
			except KeyError:
				print 'Received KeyError attemtping to process answer ' + str(row.get('answer_id'))

		if data['has_more'] is False:
			print 'Finished constructing list; number of answers: ' + str(len(ans_list))
			keep_running = False
		elif data['quota_remaining'] == 0:
			print 'Quota exhausted; stopping'
			return -1
		else:
			time.sleep(0.5)
			if 'backoff' in data:
				print 'Backoff: ' + data['backoff'] + 's'
				time.sleep(data['backoff'])
			page_num += 1
			keep_running = True

	base = 0
	keep_running = (len(ans_list) > 0)
	while keep_running:
		print 'Processing answers ' + str(base) + ' to ' + str(base+99)
		url = construct_ans_content_url(ans_list[base:base+100])
		data = get_json(url)

		if 'error_id' in data:
			print_error(url, data)
			return -1

		items = data['items']

		for row in items:
			try:
				values = (row['answer_id'],
					row['creation_date'],
					row['question_id'],
					row['body'],
					row['score']);

				cursor.execute('INSERT INTO answer VALUES (?, ?, ?, ?, ?)', values)
			except KeyError:
				print 'Received KeyError attempting to process answer ' + str(row.get('answer_id'))

		base += 100
		if base >= len(ans_list):
			print 'Finished reading answers'
			keep_running = False
		elif data['quota_remaining'] == 0:
			print 'Quota exhausted; stopping'
			return -1
		else:
			time.sleep(0.5)
			if 'backoff' in data:
				print 'Backoff: ' + data['backoff'] + 's'
				time.sleep(data['backoff'])
			keep_running = True

	return 0


def main(start_str, end_str):
	try:
		date_format = '%Y/%m/%d'
		start_date = str(get_timestamp(start_str))
		end_date = str(get_timestamp(end_str) + DAY_SECS)
	except:
		print 'Failed to convert supplied date into timestamp'
		return

	db = sqlite3.connect(DB_PATH, isolation_level=None)
	ret = get_questions(start_date, end_date, db)
	if ret >= 0: #no error
		ret = get_answers(start_date, end_date, db)
	if ret < 0:
		print 'Script terminated prematurely due to API issues'
	db.close()


if __name__ == '__main__':
	if len(sys.argv) != 3:
		print 'Usage: python importso.py start_date end_date'
		print 'Date format: YYYY/MM/DD'
	else:
		main(sys.argv[1], sys.argv[2])
